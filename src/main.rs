mod types;

use async_std::prelude::*;
use tide::{Request, Result, Response, StatusCode};

use std::collections::{BTreeMap, BTreeSet};
use crate::types::*;
use crate::types::DocValue::Blob;
use std::io;
use std::sync::Arc;
use async_std::sync::RwLock;
use std::rc::Rc;

// trait RawDb {
//     type Txn;
//     fn begin() -> Self::Txn;
//     fn set(&mut self, txn: Self::Txn, key: &str, val: &[u8]);
//     // TODO: Tweak type here
//     fn get(&self, txn: Self::Txn, key: &str) -> Option<&Vec<u8>>;
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
    // this will keep things a bit simpler.
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


    fn branch_contains_version(&self, target: Order, branch: &[Order]) -> bool {
        self.raw_branch_contains_version(target, branch, None)
    }

    fn branch_contains_doc_version(&self, target: Order, branch: &[Order], at_id: &DocId) -> bool {
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
                        assert!(ops.branch_contains_doc_version(*p, &doc_branch[..], &doc_op.id));
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

    fn apply_backwards(&mut self, ops: &OpDb, order: Order) {
        let op = ops.operation_by_order(order);
        // let prev_branch = self.branch;

        // Remove the operation from the branch.
        // TODO: Consider adding a dedicated method for this for symmetry with advance_branch_by_op.
        let branch = &mut self.branch;
        let this_idx = branch.iter().position(|o| *o == order).unwrap();
        let new_len = branch.len() - 1;
        if this_idx < new_len {
            branch[this_idx] = branch[new_len];
        }
        branch.truncate(new_len);

        // Add operations from the parents back.
        for p in &op.parents {
            if !ops.branch_contains_version(*p, &branch[..]) {
                branch.push(*p);
            }
        }

        // And update the data
        for doc_op in &op.doc_ops {
            let prev_vals = self.get_cloned(&doc_op.id);

            // The values should instead contain:
            // - Everything in prev_vals not including op.version
            // - All the objects named in parents that aren't superseded by another
            //   document version

            // Remove this operation's contribution to the value.
            // TODO: Also a lot of allocation going on here!
            let mut new_vals: DbValue = prev_vals.into_iter()
                .filter(|v| v.order != order).collect();
            let doc_branch: Vec<Order> = new_vals.iter().map(|v| v.order).collect();

            // And add back all the parents that aren't dominated by another value already.
            for p in &doc_op.parents {
                if !ops.branch_contains_doc_version(*p, &doc_branch[..], &doc_op.id) {
                    if *p == ROOT_ORDER {
                        // If all we have is the root, we'll delete the key.
                        assert!(new_vals.is_empty());
                    } else {
                        let parent_op = ops.operation_by_order(*p);
                        let parent_docop = doc_op_entry(&parent_op.doc_ops[..], &doc_op.id).unwrap();
                        new_vals.push(DbValueSingle {
                            order: *p,
                            value: parent_docop.patch.clone()
                        });
                    }
                }
            }

            // This is a bit dirty. We don't want to store any value if its just the root.
            if new_vals.is_empty() {
                self.docs.remove(&doc_op.id);
            } else {
                self.docs.insert(doc_op.id.clone(), new_vals);
            }
        }
    }
}


#[derive(Debug)]
struct MemDb {
    op_db: OpDb,
    view: ViewDb,
}

impl MemDb {
    fn new() -> Self {
        MemDb {
            op_db: OpDb::new(),
            view: ViewDb::new()
        }
    }

    fn apply_and_advance(&mut self, op: &RemoteOperation) -> Order {
        let order = self.op_db.add_operation(&op);
        self.view.apply_forwards(&self.op_db, order);
        order
    }
}


impl DocValue {
    fn to_bytes(&self) -> &[u8] {
        match self {
            DocValue::None => "None".as_bytes(),
            DocValue::Blob(bytes) => &bytes[..]
        }
    }
}

fn main() -> io::Result<()> {
    // let mut op_db = OpDb::new();
    // let mut view = ViewDb::new();

    let mut db = MemDb::new();

    println!("Db: {:?}", db);
    println!("Doc: {:?}", db.view.get_cloned(&"hi".to_string()));


    let op = RemoteOperation {
        version: Version {
            agent: 0,
            seq: 0
        },
        succeeds: None,
        parents: vec!(ROOT_VERSION),
        doc_ops: vec!(RemoteDocOp {
            id: "hi".to_string(),
            patch: Blob("hi there".as_bytes().to_vec()),
            parents: vec!(ROOT_VERSION)
        })
    };

    db.apply_and_advance(&op);
    // let order = db.op_db.add_operation(&op);
    // db.view.apply_forwards(&db.op_db, order);

    println!("---------");
    println!("Db: {:?}", db);
    println!("Doc: {:?}", db.view.get_cloned(&"hi".to_string()));

    // view.apply_backwards(&op_db, order);
    //
    // println!("---------");
    // println!("Db: {:?}", db);
    // println!("Doc: {:?}", view.get_cloned(&"hi".to_string()));

    // let state = Arc::new(db);
    let state = Arc::new(RwLock::new(db));
    // let doc = state.view.get_cloned(&"hi".to_string());
    // println!("doc {:?}", doc);

    let mut app = tide::with_state(state);
    app.at("/doc/:key").get(|req: Request<Arc<RwLock<MemDb>>>| async move {
        let key = req.param("key")?;
        let doc = req.state().read().await.view.get_cloned(&key.to_string());
        // println!("doc {:?}", doc);

        if doc.len() == 1 {
            Ok(Response::builder(StatusCode::Ok)
                .content_type("text/plain")
                .body(doc[0].value.to_bytes())
                .build())
        } else {
            Ok(Response::from("waaah"))
        }
    });

    app.at("/doc/:key").put(|mut req: Request<Arc<RwLock<MemDb>>>| async move {
        let content = req.body_bytes().await?;
        let key = req.param("key")?;

        // We're stuck using agent 0.
        let mut state = req.state().write().await;
        let succeeds = state.op_db.max_seq(0);
        let seq = match succeeds {
            None => 0,
            Some(i) => i + 1
        };
        let doc_succeeds: Vec<Version> = state.view.get_cloned(&key.to_string())
            .iter()
            .map(|v| v.order)
            .map(|order| state.op_db.order_to_version(order))
            .cloned()
            .collect();
        let parents: Vec<Version> = state.view.branch.iter()
            .map(|order| state.op_db.order_to_version(*order)).cloned().collect();

        let op = RemoteOperation {
            version: Version { agent: 0, seq },
            succeeds,
            parents,
            doc_ops: vec!(RemoteDocOp {
                id: key.to_string(),
                patch: Blob(content),
                parents: doc_succeeds
            })
        };

        let order = state.apply_and_advance(&op);
        let version = state.op_db.order_to_version(order);

        Ok(Response::from("ok"))
    });

    async_std::task::block_on(async {
        app.listen("0.0.0.0:4000").await
    })
}
