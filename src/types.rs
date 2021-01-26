
pub type Order = u64;
pub type Seq = u64;
pub type DocId = String;
pub type Agent = u32;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RemoteVersion {
    pub agent: String,
    pub seq: Seq
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LocalVersion {
    /** The agent is locally mapped from a string to a unique incrementing integer. */
    pub agent: Agent,
    pub seq: Seq
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOperation {
    pub version: RemoteVersion,
    /** Usually version.seq - 1. This allows sparse versions. u64 max for first version. */
    pub succeeds: Option<Seq>,

    pub parents: Vec<RemoteVersion>,
    pub doc_ops: Vec<RemoteDocOp>
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteDocOp {
    pub id: DocId,
    pub patch: DocPatch,
    pub parents: Vec<RemoteVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalOperation {
    pub order: Order,
    pub version: LocalVersion,
    pub parents: Vec<Order>,
    pub doc_ops: Vec<LocalDocOp>,

    /** Order of previous version from this agent. Not sure if this is necessary... */
    pub succeeds: Option<Order>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalDocOp {
    pub id: DocId,
    pub patch: DocPatch,
    pub parents: Vec<Order>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocValue {
    None,
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DbValueSingle {
    pub order: Order,
    pub value: DocValue,
}
pub(crate) type DbValue = Vec<DbValueSingle>;

type DocPatch = DocValue;
// #[derive(Clone, Debug, PartialEq, Eq)]
// pub enum DocPatch {
//     Replace(DocValue),
// }