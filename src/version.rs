use crate::types::*;
use std::collections::BTreeMap;
use crate::ROOT_AGENT;

/**
 * Simple structure which maps external agent strings to local agent IDs and back
 */
#[derive(Debug, Default)]
pub(crate) struct AgentMap {
    remote_to_local: BTreeMap<String, Agent>,
    local_to_remote: Vec<String>
}

pub const ROOT_AGENT_STR: &str = "ROOT";

impl AgentMap {
    pub fn new() -> Self {
        Self::default()
    }

    fn to_local(&mut self, ext: &str) -> Agent {
        if ext == ROOT_AGENT_STR { return ROOT_AGENT; }

        let local = self.remote_to_local.get(ext);
        local.cloned().unwrap_or_else(|| {
            let id = self.local_to_remote.len() as Agent;
            // Ideally we could remove the double allocate here, but this is off the hot path so its
            // not a big deal.
            self.local_to_remote.push(ext.to_string());
            self.remote_to_local.insert(ext.to_string(), id);

            id
        })
    }

    fn try_to_local(&self, ext: &str) -> Option<Agent> {
        if ext == ROOT_AGENT_STR { return Some(ROOT_AGENT); }
        self.remote_to_local.get(ext).cloned()
    }

    fn to_remote(&self, agent: Agent) -> &str {
        if agent == ROOT_AGENT { return ROOT_AGENT_STR; }
        &self.local_to_remote[agent as usize]
    }
}

impl LocalVersion {
    pub(crate) fn to_remote(&self, agent_map: &AgentMap) -> RemoteVersion {
        RemoteVersion {
            agent: agent_map.to_remote(self.agent).to_string(),
            seq: self.seq
        }
    }
}

impl RemoteVersion {
    pub(crate) fn try_to_local(&self, agent_map: &AgentMap) -> Option<LocalVersion> {
        agent_map.try_to_local(&self.agent)
            .map(|agent| {
                LocalVersion {
                    agent,
                    seq: self.seq
                }
            })
    }

    pub(crate) fn to_local_mut(&self, agent_map: &mut AgentMap) -> LocalVersion {
        LocalVersion {
            agent: agent_map.to_local(&self.agent),
            seq: self.seq
        }
    }

    pub(crate) fn encode(&self) -> String {
        let mut buf = self.agent.as_bytes().to_vec();
        buf.extend(&self.seq.to_be_bytes());
        base64::encode(buf)
    }
}

