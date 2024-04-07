use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::CheckIsLeaderError;
use openraft::error::Infallible;
use openraft::error::RaftError;
use web::Json;

use crate::raft::Request;
use crate::raft::TypeConfig;
use crate::raft_network::AppData;
use crate::KvsEngine;

/**
 * AppDatalication API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
#[post("/write")]
pub async fn write(app: Data<AppData>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[post("/read")]
pub async fn read(app: Data<AppData>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let state_machine = app.state_machine_store.state_machine.read().await;
    let key = req.0;
    let value = state_machine.data.get(key).await.unwrap();

    let res: Result<String, Infallible> = Ok(value.unwrap_or_default());
    Ok(Json(res))
}

#[post("/consistent_read")]
pub async fn consistent_read(
    app: Data<AppData>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let ret = app.raft.ensure_linearizable().await;

    match ret {
        Ok(_) => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.data.get(key).await.unwrap();

            let res: Result<String, RaftError<TypeConfig, CheckIsLeaderError<TypeConfig>>> =
                Ok(value.unwrap_or_default());
            Ok(Json(res))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}

// --- Cluster management

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
#[post("/add-learner")]
pub async fn add_learner(
    app: Data<AppData>,
    req: Json<(crate::raft::NodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = openraft::BasicNode {
        addr: req.0 .1.clone(),
    };
    let res = app.raft.add_learner(node_id, node, true).await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[post("/change-membership")]
pub async fn change_membership(
    app: Data<AppData>,
    req: Json<std::collections::BTreeSet<crate::raft::NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, false).await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[post("/init")]
pub async fn init(app: Data<AppData>) -> actix_web::Result<impl Responder> {
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(
        app.id,
        openraft::BasicNode {
            addr: app.addr.clone(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

// TODO
//#[get("/metrics")]
//pub async fn metrics(app: Data<AppData>) -> actix_web::Result<impl Responder> {
//    let metrics = app.raft.metrics().borrow().clone();
//
//    let res: Result<RaftMetrics<TypeConfig>, Infallible> = Ok(metrics);
//    Ok(Json(res))
//}

// --- Raft communication

#[post("/raft-vote")]
pub async fn vote(
    app: Data<AppData>,
    req: Json<openraft::raft::VoteRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<AppData>,
    req: Json<openraft::raft::AppendEntriesRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<AppData>,
    req: Json<openraft::raft::InstallSnapshotRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}
