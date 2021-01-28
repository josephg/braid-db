use std::collections::BTreeMap;
use crate::types::*;
use crate::{ROOT_ORDER, DEEP_CHECK, doc_op_entry};
use crate::op_db::OpDb;

#[derive(Debug)]
pub struct ViewDb {
    pub(crate) branch: Vec<Order>,
    docs: BTreeMap<DocId, DbValue>,
}

impl Default for ViewDb {
    fn default() -> Self {
        ViewDb {
            branch: vec!(ROOT_ORDER),
            docs: BTreeMap::new()
        }
    }
}

impl ViewDb {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_cloned(&self, key: &DocId) -> DbValue {
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

    pub(crate) fn branch_as_versions(&self, ops: &OpDb) -> Vec<LocalVersion> {
        self.branch.iter().map(|o| {
            ops.order_to_version(*o).clone()
        }).collect()
    }

    pub(crate) fn apply_forwards(&mut self, ops: &OpDb, order: Order) {
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

    pub(crate) fn apply_backwards(&mut self, ops: &OpDb, order: Order) {
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

