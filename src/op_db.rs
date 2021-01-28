use crate::types::*;
use crate::version::AgentMap;
use std::collections::{BTreeMap, BTreeSet};
use crate::{ROOT_ORDER, ROOT_AGENT, ROOT_VERSION, DEEP_CHECK, doc_op_entry};


#[derive(Debug)]
pub struct OpDb {
    pub(crate) agent_map: AgentMap,

    // At some point I'll need to merge everything into one kv map but for now
    // this will keep things a bit simpler.
    // ops: BTreeMap<Order, ()>,
    ops: Vec<LocalOperation>,

    // Ugh, I can't use this because btreemap has no way to
    // version_to_order: BTreeMap<RemoteVersion, ()>,
    version_to_order: BTreeMap<LocalVersion, Order>,
    // map: BTreeMap<Vec<u8>, Vec<u8>>

    // For easy syncing. This only moves forward!
    frontier: Vec<Order>,
}


impl Default for OpDb {
    fn default() -> Self {
        OpDb {
            agent_map: AgentMap::new(),
            version_to_order: BTreeMap::new(),
            ops: Vec::new(),
            frontier: vec!(ROOT_ORDER)
        }
    }
}

fn entry_before<'a, K: Ord, V>(map: &'a BTreeMap<K, V>, key: &K) -> Option<&'a K> {
    let mut iter = map.range(..key);
    iter.next_back().map(|(k, _)| k)
}


impl OpDb {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /**
     * Gets the max known sequence number for the specified agent. None if the
     * agent is not known in the database.
     */
    pub(crate) fn max_seq(&self, agent: Agent) -> Option<Seq> {
        let end = LocalVersion {agent, seq: Seq::MAX};
        entry_before(&self.version_to_order, &end)
            .and_then(|v| {
                if v.agent == agent {Some(v.seq)} else {None}
            })
    }

    /** Fetch the operation with the specified order */
    pub(crate) fn operation_by_order(&self, order: Order) -> &LocalOperation {
        assert_ne!(order, ROOT_ORDER, "Cannot fetch root operation");
        &self.ops[order as usize]
    }

    /** Fetch the operation with the specified remote version */
    pub(crate) fn operation_by_version(&self, version: &LocalVersion) -> Option<&LocalOperation> {
        self.version_to_order(version)
            .map(|order| self.operation_by_order(order))
    }

    pub(crate) fn version_to_order(&self, version: &LocalVersion) -> Option<Order> {
        if version.agent == ROOT_AGENT { Some(ROOT_ORDER) }
        else {
            self.version_to_order.get(version).cloned()
        }
    }

    pub(crate) fn remote_version_to_order_mut(&mut self, version: &RemoteVersion) -> Option<Order> {
        let local = version.to_local_mut(&mut self.agent_map);
        self.version_to_order(&local)
    }

    pub(crate) fn remote_version_to_order(&self, version: &RemoteVersion) -> Option<Order> {
        let local = version.try_to_local(&self.agent_map).unwrap();
        self.version_to_order(&local)
    }

    pub(crate) fn order_to_version(&self, order: Order) -> &LocalVersion {
        if order == ROOT_ORDER { &ROOT_VERSION }
        else {
            &self.operation_by_order(order).version
        }
    }

    pub(crate) fn order_to_remote_version(&self, order: Order) -> RemoteVersion {
        self.order_to_version(order).to_remote(&self.agent_map)
    }

    // ***** Serious utilities


    pub(crate) fn branch_contains_version(&self, target: Order, branch: &[Order]) -> bool {
        self.raw_branch_contains_version(target, branch, None)
    }

    pub(crate) fn branch_contains_doc_version(&self, target: Order, branch: &[Order], at_id: &DocId) -> bool {
        self.raw_branch_contains_version(target, branch, Some(at_id))
    }

    fn raw_branch_contains_version(&self, target: Order, branch: &[Order], at_id: Option<&DocId>) -> bool {
        if DEEP_CHECK && at_id.is_some() {
            // When we're in document mode, all operations named in the branch must
            // contain an operation modifying the document ID.
            for &o in branch {
                let op = self.operation_by_order(o);
                assert!(op.doc_ops.iter().any(|op| &op.id == at_id.unwrap()));
            }
        }

        // Order matters between these two lines because of how this is used in applyBackwards.
        if branch.len() == 0 { return false; }
        if target == ROOT_ORDER || branch.contains(&target) { return true; }

        // This works is via a DFS from the operation with a higher localOrder looking
        // for the Order of the smaller operation.
        let mut visited = BTreeSet::<Order>::new();
        let mut found = false;

        // LIFO queue. We could use a priority queue here but I'm not sure it'd be any
        // faster in practice.
        let mut queue = branch.to_vec();
        queue.sort_by(|a, b| b.cmp(a)); // descending so we hit the lowest first.

        while !found {
            let order = match queue.pop() {
                Some(o) => o,
                None => {break}
            };

            if order <= target || order == ROOT_ORDER {
                if order == target { found = true; }
                continue;
            }

            if visited.contains(&order) { continue; }
            visited.insert(order);

            let op = self.operation_by_order(order);

            match &at_id {
                None => {
                    // Operation versions. Add all of op's parents to the queue.
                    queue.extend(op.parents.iter());

                    // Ordered so we hit this next. This isn't necessary, the succeeds field
                    // will just often be smaller than the parents.
                    if let Some(succeeds) = op.succeeds {
                        queue.push(succeeds);
                    }
                },
                Some(at_id) => {
                    // We only care about the operations which modified this key. This is much
                    // faster.
                    let doc_op = doc_op_entry(&op.doc_ops[..], at_id)
                        .expect("Missing doc op entry in operation");
                    queue.extend(doc_op.parents.iter());
                }
            }
        }

        found
    }

    /** Add an operation into the operation database. */
    pub(crate) fn add_operation(&mut self, op: &RemoteOperation) -> Order {
        assert!(op.parents.len() > 0, "Operation parents field must not be empty");
        let local_version = op.version.to_local_mut(&mut self.agent_map);

        if let Some(&order) = self.version_to_order.get(&local_version) {
            // The operation is already in the database.
            return order;
        }

        // Check that all of this operation's parents are already present.
        // println!("inserting {:?}", op);
        let parent_orders = op.parents.iter().map(|v| {
            self.remote_version_to_order_mut(v)
                .expect("Operation's parent missing in op db")
        }).collect();

        // Ok looking good. Lets assign an order and merge.
        let new_order = self.ops.len() as Order;

        let local_op = LocalOperation {
            order: new_order,
            version: local_version,
            parents: parent_orders,
            doc_ops: op.doc_ops.iter().map(|doc_op| LocalDocOp {
                id: doc_op.id.clone(),
                parents: doc_op.parents.iter().map(|v| {
                    self.remote_version_to_order_mut(&v).expect("Docop parent missing")
                }).collect(),
                patch: doc_op.patch.clone(),
            }).collect(),
            succeeds: op.succeeds.map(|seq| self.version_to_order(&LocalVersion {
                agent: local_version.agent,
                seq
            }).expect("Predecessor missing in database"))
        };

        // TODO: Avoid allocation here.
        self.frontier = self.advance_branch_by_op(&self.frontier[..], &local_op);

        // And save the new operation in the store.
        self.ops.push(local_op);
        self.version_to_order.insert(local_version, new_order);

        new_order
    }

    // I'm not entirely sure where this function should live.
    // TODO: Consider rewriting this to avoid the allocation.
    pub(crate) fn advance_branch_by_op(&self, branch: &[Order], op: &LocalOperation) -> Vec<Order> {
        let order = op.order;
        // Check the operation fits. The operation should not be in the branch, but
        // all the operation's parents should be.
        // println!("bcv {:?} {:?}", order, branch);
        assert!(!self.branch_contains_version(order, branch));
        for &parent in op.parents.iter() {
            assert!(self.branch_contains_version(parent, branch));
        }

        // Every version named by branch is either:
        // - Equal to a branch in the new operation's parents (in which case remove it)
        // - Or newer than a branch in the operation's parents (in which case keep it)
        // If there were any versions which are older, we would have aborted above.
        let mut b: Vec<Order> = branch.iter().filter(|o| !op.parents.contains(o))
            .copied().collect();
        b.push(order);
        b
    }
}
