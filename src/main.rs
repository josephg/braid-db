mod types;

use std::collections::{BTreeMap, BTreeSet};
use crate::types::*;
use crate::types::DocValue::Blob;

// trait RawDb {
//     type Txn;
//     fn begin() -> Self::Txn;
//     fn set(&mut self, txn: Self::Txn, key: &str, val: &[u8]);
//     // TODO: Tweak type here
//     fn get(&self, txn: Self::Txn, key: &str) -> Option<&Vec<u8>>;
// }


// thread_local! {
//     static ROOT_VERSION: RemoteVersion = RemoteVersion {
//         agent: "ROOT".to_string(),
//         seq: Seq::MAX
//     };
// }
const ROOT_AGENT: Agent = Agent::MAX;
const ROOT_ORDER: Order = Order::MAX;
const ROOT_VERSION: Version = Version {
    agent: ROOT_AGENT,
    seq: 0
};

const DEEP_CHECK: bool = true;

#[derive(Debug)]
struct OpDb {
    // At some point I'll need to merge everything into one kv map but for now
    // this'll keep things a bit simpler.
    // ops: BTreeMap<Order, ()>,
    ops: Vec<LocalOperation>,

    // Ugh, I can't use this because btreemap has no way to
    // version_to_order: BTreeMap<RemoteVersion, ()>,
    version_to_order: BTreeMap<Version, Order>,
    // map: BTreeMap<Vec<u8>, Vec<u8>>

    // For easy syncing. This only moves forward!
    frontier: Vec<Order>,
}

#[derive(Debug)]
struct ViewDb {
    branch: Vec<Order>,
    docs: BTreeMap<DocId, DbValue>,
}

// struct MemDb {
//     ops: OpDb,
//     view: ViewDb,
// }

fn entry_before<'a, K: Ord, V>(map: &'a BTreeMap<K, V>, key: &K) -> Option<&'a K> {
    let mut iter = map.range(..key);
    iter.next_back().map(|(k, _)| k)
}

fn doc_op_entry<'a>(entries: &'a[LocalDocOp], needle: &DocId) -> Option<&'a LocalDocOp> {
    entries.iter().find(|doc_op| &doc_op.id == needle)
}

impl OpDb {
    fn new() -> Self {
        OpDb {
            version_to_order: BTreeMap::new(),
            ops: Vec::new(),
            frontier: vec!(ROOT_ORDER)
        }
    }

    /**
     * Gets the max known sequence number for the specified agent. None if the
     * agent is not known in the database.
     */
    fn max_seq(&self, agent: Agent) -> Option<Seq> {
        let end = Version {agent, seq: Seq::MAX};
        entry_before(&self.version_to_order, &end)
        .and_then(|v| {
            if v.agent == agent {Some(v.seq)} else {None}
        })
    }

    /** Fetch the operation with the specified order */
    fn operation_by_order(&self, order: Order) -> &LocalOperation {
        assert_ne!(order, ROOT_ORDER, "Cannot fetch root operation");
        &self.ops[order as usize]
    }

    /** Fetch the operation with the specified remote version */
    fn operation_by_version(&self, version: &Version) -> Option<&LocalOperation> {
        self.version_to_order(version)
        .map(|order| self.operation_by_order(order))
    }

    fn version_to_order(&self, version: &Version) -> Option<Order> {
        if version.agent == ROOT_AGENT { Some(ROOT_ORDER) }
        else {
            self.version_to_order.get(version).cloned()
        }
    }

    fn order_to_version(&self, order: Order) -> &Version {
        if order == ROOT_ORDER { &ROOT_VERSION }
        else {
            &self.operation_by_order(order).version
        }
    }

    // ***** Serious utilities

    fn branch_contains_version(&self, target: Order, branch: &[Order], at_id: Option<&DocId>) -> bool {
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
    fn add_operation(&mut self, op: &RemoteOperation) -> Order {
        assert!(op.parents.len() > 0, "Operation parents field must not be empty");

        if let Some(&order) = self.version_to_order.get(&op.version) {
            // The operation is already in the database.
            return order;
        }

        // Check that all of this operation's parents are already present.
        let parent_orders = op.parents.iter().map(|v| {
            self.version_to_order(v)
                .expect("Operation's parent missing in op db")
        }).collect();

        // Ok looking good. Lets assign an order and merge.
        let new_order = self.ops.len() as Order;

        let local_op = LocalOperation {
            order: new_order,
            version: op.version.clone(),
            parents: parent_orders,
            doc_ops: op.doc_ops.iter().map(|doc_op| LocalDocOp {
                id: doc_op.id.clone(),
                parents: doc_op.parents.iter().map(|v| {
                   self.version_to_order(&v).expect("Docop parent missing")
                }).collect(),
                patch: doc_op.patch.clone(),
            }).collect(),
            succeeds: op.succeeds.map(|seq| self.version_to_order(&Version {
                agent: op.version.agent,
                seq
            }).expect("Predecessor missing in database"))
        };

        // TODO: Avoid allocation here.
        self.frontier = self.advance_branch_by_op(&self.frontier[..], &local_op);

        // And save the new operation in the store.
        self.ops.push(local_op);
        self.version_to_order.insert(op.version, new_order);

        new_order
    }

    // I'm not entirely sure where this function should live.
    // TODO: Consider rewriting this to avoid the allocation.
    fn advance_branch_by_op(&self, branch: &[Order], op: &LocalOperation) -> Vec<Order> {
        let order = op.order;
        // Check the operation fits. The operation should not be in the branch, but
        // all the operation's parents should be.
        // println!("bcv {:?} {:?}", order, branch);
        assert!(!self.branch_contains_version(order, branch, None));
        for &parent in op.parents.iter() {
            assert!(self.branch_contains_version(parent, branch, None));
        }

        // Every version named by branch is either:
        // - Equal to a branch in the new operation's parents (in which case remove it)
        // - Or newer than a branch in the operation's parents (in which case keep it)
        // If there were any versions which are older, we would have aborted above.
        let mut b: Vec<Order> = branch.iter().filter(|o| op.parents.contains(o))
            .copied().collect();
        b.push(order);
        b
    }
}

impl ViewDb {
    fn new() -> Self {
        ViewDb {
            branch: vec!(ROOT_ORDER),
            docs: BTreeMap::new()
        }
    }

    fn get_cloned(&self, key: &DocId) -> DbValue {
        // Every document implicitly exists, with a null value.
        // TODO: Refactor to avoid .clone().
        self.docs.get(key).cloned().unwrap_or_else(|| {
            vec!(DbValueSingle {
                order: ROOT_ORDER,
                value: DocValue::None
            })
        })
    }

    // TODO:
    // fn get_remote_value(&self, key: &DocId) ->

    fn branch_as_versions(&self, ops: &OpDb) -> Vec<Version> {
        self.branch.iter().map(|o| {
            ops.order_to_version(*o).clone()
        }).collect()
    }

    fn apply_forwards(&mut self, ops: &OpDb, order: Order) {
        let op = ops.operation_by_order(order);

        let new_branch = ops.advance_branch_by_op(&self.branch[..], op);
        self.branch = new_branch;

        for doc_op in &op.doc_ops {
            let prev_vals = self.get_cloned(&doc_op.id);

            // The doc op's parents field contains a subset of the versions present in
            // oldVal.
            // - Any entries in prevVals that aren't named in the new operation are kept
            // - And any entries in parents that aren't directly named in prevVals must
            //   be ancestors of the current document value. This indicates a conflict,
            //   and we'll keep everything.

            if DEEP_CHECK {
                // Check ancestry. Every parent of this operation (parents) must
                // either be represented directly in prev_vals or be dominated of one of
                // them.
                for p in doc_op.parents.iter() {
                    let exists = prev_vals.iter().any(|v| v.order == *p);

                    if !exists {
                        let doc_branch: Vec<Order> = prev_vals.iter()
                            .map(|v| v.order).collect();
                        assert!(ops.branch_contains_version(*p, &doc_branch[..], Some(&doc_op.id)));
                    }
                }
            }

            let mut new_vals: DbValue = vec!(DbValueSingle {
                order,
                value: doc_op.patch.clone()
            });

            for old_entry in prev_vals {
                if !doc_op.parents.contains(&old_entry.order) {
                    // Keep!
                    new_vals.push(old_entry);
                }
            }

            // If there's multiple conflicting versions, keep them sorted for easier comparisons in
            // the fuzzer.
            new_vals.sort_by_key(|v| v.order);
            self.docs.insert(doc_op.id.clone(), new_vals);
            // TODO: Should be a way to avoid the clone when updating.
            // *self.docs.get_mut(&doc_op.id).unwrap() = new_vals;

            // TODO: And update listeners.
        }
    }
}

fn main() {
    let mut opdb = OpDb::new();
    let mut view = ViewDb::new();

    println!("Ops: {:?}", opdb);
    println!("View: {:?}", view);
    println!("Doc: {:?}", view.get_cloned(&"hi".to_string()));


    let op = RemoteOperation {
        version: Version {
            agent: 0,
            seq: 0
        },
        succeeds: None,
        parents: vec!(ROOT_VERSION),
        doc_ops: vec!(RemoteDocOp {
            id: "hi".to_string(),
            patch: Blob(vec!(1,2,3)),
            parents: vec!(ROOT_VERSION)
        })
    };

    let order = opdb.add_operation(&op);
    view.apply_forwards(&opdb, order);

    println!("Ops: {:?}", opdb);
    println!("View: {:?}", view);
    println!("Doc: {:?}", view.get_cloned(&"hi".to_string()));


}
