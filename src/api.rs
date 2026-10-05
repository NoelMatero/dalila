use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Result, bail};
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use membership::dissemination::GossipQueue;
use membership::node::{Incarnation, LocalNode, NodeId};
use membership::state::MemberTable;
use membership::wire::check_tags;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::proxy::{self, Proxies, ProxyInfo};

/// Shortest `--api-token` taken. Guessing it means asking the API, once per
/// guess, so this matters less than the cluster key's length. But a
/// localhost API answers fast, and 32 costs nothing.
const MIN_TOKEN_LEN: usize = 32;

pub struct Config {
    /// Where the API answers.
    pub listen: SocketAddr,
    /// What a change has to come with. `None`: changes are turned off.
    pub token: Option<String>,
}

/// The error never repeats the token, since errors end up on screens and in
/// logs.
pub fn check_token(token: &str) -> Result<()> {
    if token.len() < MIN_TOKEN_LEN {
        bail!(
            "--api-token is {} characters, needs at least {MIN_TOKEN_LEN}. \
             `openssl rand -hex 32` makes a good one",
            token.len()
        );
    }
    Ok(())
}

/// What a handler needs. Cloned for every request, which is cheap: every
/// field is a handle to shared state, not a copy of it.
#[derive(Clone)]
struct Api {
    node: LocalNode,
    table: MemberTable,
    queue: GossipQueue,
    proxies: Proxies,
    token: Option<Arc<str>>,
}

/// Answer HTTP requests about this node and the cluster, and, with a token,
/// change this node while it runs.
///
/// Reading needs no token: it's what an external proxy polls, and all it
/// shows is addresses and states. Changing needs one, because a change can
/// open a port or move traffic. Nothing changed here is saved: a restart
/// goes back to the flags.
pub async fn start_api(
    listener: TcpListener,
    node: LocalNode,
    table: MemberTable,
    queue: GossipQueue,
    proxies: Proxies,
    token: Option<String>,
) {
    let api = Api {
        node,
        table,
        queue,
        proxies,
        token: token.map(Into::into),
    };

    let reads = Router::new()
        .route("/backends", get(backends))
        .route("/node", get(node_status))
        .route("/proxies", get(list_proxies));

    // the token check wraps every route in here and nothing outside it, so a
    // new change can't be added without it by forgetting a line
    let changes = Router::new()
        .route("/tags", put(set_tags))
        .route("/drain", post(drain))
        .route("/undrain", post(undrain))
        .route("/proxies", post(add_proxy))
        .route("/proxies/{listen}", delete(remove_proxy))
        .route_layer(middleware::from_fn_with_state(api.clone(), authorize));

    let app = reads.merge(changes).with_state(api);

    // runs until aborted. accept errors are handled inside, so this only
    // returns if something is badly wrong
    if let Err(err) = axum::serve(listener, app).await {
        warn!("api stopped: {err}");
    }
}

/// An error, as `{"error": "..."}` with a status code.
struct Failure(StatusCode, String);

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

/// Let a change through only with `Authorization: Bearer <token>`.
async fn authorize(State(api): State<Api>, request: Request, next: Next) -> Response {
    let Some(token) = &api.token else {
        return Failure(
            StatusCode::FORBIDDEN,
            "changes are off: start the node with --api-token".into(),
        )
        .into_response();
    };

    let given = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    match given {
        Some(given) if same(given.as_bytes(), token.as_bytes()) => next.run(request).await,
        _ => Failure(StatusCode::UNAUTHORIZED, "missing or wrong token".into()).into_response(),
    }
}

/// `a == b`, but always looking at every byte. An `==` stops at the first
/// difference, and timing that is how a token gets guessed a byte at a time.
/// The length isn't hidden, which is fine: it says nothing about the bytes.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// `GET /backends?tag=web`: only members with that tag, like `--proxy-to`.
#[derive(Deserialize)]
struct Filter {
    tag: Option<String>,
}

/// One entry in `GET /backends`. An object rather than a bare string, so a
/// field can be added later without breaking anyone reading it.
#[derive(Serialize)]
struct Backend {
    addr: SocketAddr,
}

/// Every member that can take work right now, this node included if it can.
/// The same list the built-in proxy uses, from the same function, so the two
/// never disagree.
///
/// A member whose app went down a moment ago may still be listed: the news
/// takes a second or two to arrive. A caller should treat a refused connect
/// the way the built-in proxy does, and try the next one.
async fn backends(State(api): State<Api>, Query(filter): Query<Filter>) -> Json<Vec<Backend>> {
    let backends = proxy::backends(
        &api.node,
        &api.table,
        api.proxies.backend_port(),
        filter.tag.as_deref(),
    )
    .into_iter()
    .map(|addr| Backend { addr })
    .collect();
    Json(backends)
}

/// `GET /node`, and the answer to every change: this node as it stands.
#[derive(Serialize)]
struct NodeView {
    id: NodeId,
    addr: SocketAddr,
    incarnation: Incarnation,
    /// Whether other nodes send this one work: `app_up` and not `drained`.
    ready: bool,
    app_up: bool,
    drained: bool,
    tags: Vec<String>,
    proxies: Vec<ProxyInfo>,
}

fn view(api: &Api) -> NodeView {
    let status = api.node.status();
    NodeView {
        id: api.node.id,
        addr: api.node.addr,
        incarnation: status.incarnation,
        ready: status.ready,
        app_up: status.app_up,
        drained: status.drained,
        tags: status.tags,
        proxies: api.proxies.list(),
    }
}

async fn node_status(State(api): State<Api>) -> Json<NodeView> {
    Json(view(&api))
}

/// Tell the cluster about a change to this node, the way the health check
/// does when the app goes up or down.
fn announce(api: &Api) {
    // table.len() counts everyone but us
    api.queue.push(api.node.alive_rumor(), api.table.len() + 1);
}

/// `PUT /tags` with `["web", "canary"]`: replace this node's tags. Every
/// proxy picking by tag sees the change once the gossip reaches it.
async fn set_tags(
    State(api): State<Api>,
    Json(tags): Json<Vec<String>>,
) -> Result<Json<NodeView>, Failure> {
    check_tags(&tags).map_err(|err| Failure(StatusCode::BAD_REQUEST, format!("{err:#}")))?;

    if api.node.set_tags(tags.clone()) {
        info!(?tags, "tags changed");
        announce(&api);
    }
    Ok(Json(view(&api)))
}

/// `POST /drain`: stop being sent work, here and everywhere else, without
/// stopping anything. Connections already open carry on.
async fn drain(State(api): State<Api>) -> Json<NodeView> {
    if api.node.set_drained(true) {
        info!("drained; asking the cluster to stop sending work");
        announce(&api);
    }
    Json(view(&api))
}

/// `POST /undrain`: take work again, as long as the app is up.
async fn undrain(State(api): State<Api>) -> Json<NodeView> {
    if api.node.set_drained(false) {
        info!("undrained; taking work again");
        announce(&api);
    }
    Json(view(&api))
}

async fn list_proxies(State(api): State<Api>) -> Json<Vec<ProxyInfo>> {
    Json(api.proxies.list())
}

/// `POST /proxies` with `{"listen": "0.0.0.0:8081", "to": "web"}`. `to` is
/// optional, as with `--proxy-to`.
#[derive(Deserialize)]
struct NewProxy {
    listen: SocketAddr,
    to: Option<String>,
}

async fn add_proxy(
    State(api): State<Api>,
    Json(new): Json<NewProxy>,
) -> Result<(StatusCode, Json<ProxyInfo>), Failure> {
    let listener = TcpListener::bind(new.listen).await.map_err(|err| {
        // taken is the common case, and usually means a proxy is already there
        let status = match err.kind() {
            ErrorKind::AddrInUse => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        };
        Failure(status, format!("could not bind {}: {err}", new.listen))
    })?;

    let listen = api
        .proxies
        .start(listener, new.to.clone())
        .map_err(|err| Failure(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok((StatusCode::CREATED, Json(ProxyInfo { listen, to: new.to })))
}

/// `DELETE /proxies/127.0.0.1:8081`: stop the proxy listening there, whether
/// the API or `--proxy` started it.
async fn remove_proxy(
    State(api): State<Api>,
    Path(listen): Path<SocketAddr>,
) -> Result<StatusCode, Failure> {
    if api.proxies.stop(&listen).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(Failure(
            StatusCode::NOT_FOUND,
            format!("no proxy listening on {listen}"),
        ))
    }
}
