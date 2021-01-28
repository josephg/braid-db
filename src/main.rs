mod types;
mod version;
mod op_db;
mod view_db;
mod httpserver;

use crate::types::*;
use crate::version::ROOT_AGENT_STR;
use std::io;
use crate::op_db::OpDb;
use crate::view_db::ViewDb;
use crate::httpserver::host;


pub(crate) const ROOT_AGENT: Agent = Agent::MAX;
pub(crate) const ROOT_ORDER: Order = Order::MAX;
pub(crate) const ROOT_VERSION: LocalVersion = LocalVersion {
    agent: ROOT_AGENT,
    seq: 0
};

pub(crate) const DEEP_CHECK: bool = true;

pub(crate) fn doc_op_entry<'a>(entries: &'a[LocalDocOp], needle: &DocId) -> Option<&'a LocalDocOp> {
    entries.iter().find(|doc_op| &doc_op.id == needle)
}



// TODO: Remove this.
#[derive(Debug, Default)]
pub struct MemDb {
    op_db: OpDb,
    view: ViewDb,
}

impl MemDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_and_advance(&mut self, op: &RemoteOperation) -> Order {
        let order = self.op_db.add_operation(&op);
        self.view.apply_forwards(&self.op_db, order);
        order
    }
}


fn main() -> io::Result<()> {
    // let mut op_db = OpDb::new();
    // let mut view = ViewDb::new();

    let mut db = MemDb::new();

    println!("Db: {:?}", db);
    println!("Doc: {:?}", db.view.get_cloned(&"hi".to_string()));

    let root_version = RemoteVersion {
        agent: ROOT_AGENT_STR.to_string(),
        seq: 0
    };
    let op = RemoteOperation {
        version: RemoteVersion {
            agent: "seph".to_string(),
            seq: 0
        },
        succeeds: None,
        parents: vec!(root_version.clone()),
        doc_ops: vec!(RemoteDocOp {
            id: "hi".to_string(),
            patch: DocValue::Blob("hi there".as_bytes().to_vec()),
            parents: vec!(root_version.clone())
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

    async_std::task::block_on(host(db))
}
