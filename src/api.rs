use std::net::SocketAddr;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use membership::node::LocalNode;
use membership::state::MemberTable;
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::warn;

use crate::proxy;

pub struct Config {
    /// Where the API answers.
    pub listen: SocketAddr,
    /// Same as the proxy's: the port every member serves the app on.
    pub backend_port: u16,
}

/// What a handler needs. Cloned for every request, which is cheap: the node
/// and the table are handles to shared state, not copies of it.
#[derive(Clone)]
struct Api {
    node: LocalNode,
    table: MemberTable,
    backend_port: u16,
}

/// One entry in `GET /backends`. An object rather than a bare string, so a
/// field can be added later without breaking anyone reading it.
#[derive(Serialize)]
struct Backend {
    addr: SocketAddr,
}

/// Answer HTTP requests asking where to send work, for a proxy other than
/// the built-in one: HAProxy, Envoy, a script that rewrites an nginx config.
///
/// Read-only, and with no auth, so anyone who can reach it can see the
/// cluster's addresses. Meant for the same machine, next to that proxy.
pub async fn start_api(
    listener: TcpListener,
    node: LocalNode,
    table: MemberTable,
    backend_port: u16,
) {
    let app = Router::new()
        .route("/backends", get(backends))
        .with_state(Api {
            node,
            table,
            backend_port,
        });

    // runs until aborted. accept errors are handled inside, so this only
    // returns if something is badly wrong
    if let Err(err) = axum::serve(listener, app).await {
        warn!("api stopped: {err}");
    }
}

/// Every member that can take work right now, this node included if it can.
/// The same list the built-in proxy uses, from the same function, so the two
/// never disagree.
///
/// A member whose app went down a moment ago may still be listed: the news
/// takes a second or two to arrive. A caller should treat a refused connect
/// the way the built-in proxy does, and try the next one.
async fn backends(State(api): State<Api>) -> Json<Vec<Backend>> {
    let backends = proxy::backends(&api.node, &api.table, api.backend_port)
        .into_iter()
        .map(|addr| Backend { addr })
        .collect();
    Json(backends)
}
