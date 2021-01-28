use crate::types::*;
use crate::MemDb;

use std::sync::Arc;
use async_std::sync::RwLock;
use tide::{Request, Response, StatusCode};

impl DocValue {
    fn to_bytes(&self) -> &[u8] {
        match self {
            DocValue::None => "None".as_bytes(),
            DocValue::Blob(bytes) => &bytes[..]
        }
    }
}

pub async fn host(db: MemDb) -> std::io::Result<()> {
    let state = Arc::new(RwLock::new(db));

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
        let doc_succeeds: Vec<RemoteVersion> = state.view.get_cloned(&key.to_string())
            .iter()
            .map(|v| v.order)
            .map(|order| state.op_db.order_to_remote_version(order))
            .collect();
        let parents: Vec<RemoteVersion> = state.view.branch.iter()
            .map(|order| state.op_db.order_to_version(*order))
            .map(|local| local.to_remote(&state.op_db.agent_map))
            .collect();

        let agent = "seph".to_string();
        let op = RemoteOperation {
            version: RemoteVersion { agent, seq },
            succeeds,
            parents,
            doc_ops: vec!(RemoteDocOp {
                id: key.to_string(),
                patch: DocValue::Blob(content),
                parents: doc_succeeds
            })
        };

        let order = state.apply_and_advance(&op);
        let version = state.op_db.order_to_remote_version(order);

        Ok(Response::builder(StatusCode::Ok)
            .header("version", version.encode())
            .body("")
            .build())
    });

    app.listen("0.0.0.0:4000").await
}
