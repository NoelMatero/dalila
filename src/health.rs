use std::net::SocketAddr;
use std::time::Duration;

use membership::dissemination::GossipQueue;
use membership::node::LocalNode;
use membership::state::MemberTable;
use tokio::net::TcpStream;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{info, warn};

/// How often a node checks its own app.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How long the app gets to accept the check's connection. The same as the
/// proxy gives a backend: an app too slow for this is too slow for a client.
const CHECK_TIMEOUT: Duration = Duration::from_secs(1);

/// Check this node's own app every second, and tell the cluster whenever the
/// answer changes.
///
/// Each node checks only itself. It's the one place the answer is certain:
/// no network in between, and one check per app instead of one per proxy.
pub async fn start_health_check(
    node: LocalNode,
    table: MemberTable,
    queue: GossipQueue,
    backend_port: u16,
) {
    // the same address a proxy on another node would use
    let app = SocketAddr::new(node.addr.ip(), backend_port);

    let mut ticks = interval(CHECK_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticks.tick().await;

        let ready = answers(app).await;
        if !node.set_ready(ready) {
            continue;
        }

        if ready {
            info!(%app, "app is answering; taking work again");
        } else {
            warn!(%app, "app isn't answering; asking the cluster to stop sending work");
        }
        // table.len() counts everyone but us
        queue.push(node.alive_rumor(), table.len() + 1);
    }
}

/// True if the app accepts a connection. Nothing is sent, and the connection
/// is closed straight away: "is anything listening?" is all this asks.
async fn answers(app: SocketAddr) -> bool {
    matches!(timeout(CHECK_TIMEOUT, TcpStream::connect(app)).await, Ok(Ok(_)))
}
