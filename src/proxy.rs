use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use membership::node::LocalNode;
use membership::state::MemberTable;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{debug, warn};

/// How long one backend gets to accept a connection before we try the next.
///
/// Short, because a client is waiting on every try. A healthy backend on the
/// same network answers a connect in well under a millisecond.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct Config {
    /// Where clients connect.
    pub listen: SocketAddr,
    /// The port every member serves the proxied service on. A member's own
    /// address is its gossip port, so this replaces it and the IP is kept.
    pub backend_port: u16,
}

/// Accept client connections and hand each one to a live member.
///
/// Works on raw TCP and never looks at the bytes, so it proxies anything:
/// HTTP, a database, a cache. The price is that it balances connections, not
/// requests. A client that keeps one connection open stays on one backend.
pub async fn start_proxy(
    listener: TcpListener,
    node: LocalNode,
    table: MemberTable,
    backend_port: u16,
) {
    // where the round-robin is up to. shared by every connection, so two
    // clients arriving at once start at different backends
    let next = Arc::new(AtomicUsize::new(0));

    loop {
        let (client, client_addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                // same reasoning as the membership accept loop: one bad accept
                // shouldn't stop the rest, and the pause stops a persistent
                // error (out of file descriptors) from spinning
                warn!("proxy accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let node = node.clone();
        let table = table.clone();
        let next = next.clone();

        tokio::spawn(async move {
            forward(client, client_addr, &node, &table, backend_port, &next).await;
        });
    }
}

/// Connect `client` to the first backend that accepts, then shuttle bytes both
/// ways until one side hangs up.
async fn forward(
    mut client: TcpStream,
    client_addr: SocketAddr,
    node: &LocalNode,
    table: &MemberTable,
    backend_port: u16,
    next: &AtomicUsize,
) {
    // read fresh for every connection, so a member that died a moment ago is
    // already gone from the list
    let backends = backends(node, table, backend_port);
    let start = next.fetch_add(1, Ordering::Relaxed);

    // every backend gets one try, starting where the last connection left off.
    // the table can be a few seconds behind reality (a member dies, and it
    // takes the detector a while to notice), so a refused connect is normal
    // and just means moving on
    for i in 0..backends.len() {
        let backend_addr = backends[(start + i) % backends.len()];

        let mut backend = match timeout(CONNECT_TIMEOUT, TcpStream::connect(backend_addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                debug!(backend = %backend_addr, "backend refused: {err}");
                continue;
            }
            Err(_) => {
                debug!(backend = %backend_addr, "backend didn't answer in time");
                continue;
            }
        };

        debug!(client = %client_addr, backend = %backend_addr, "forwarding");

        // from here on, bytes have moved, so there's no retrying: the backend
        // may have acted on half a request. a failure ends the connection and
        // the client finds out the way it would without a proxy
        if let Err(err) = copy_bidirectional(&mut client, &mut backend).await {
            debug!(client = %client_addr, backend = %backend_addr, "connection ended: {err}");
        }
        return;
    }

    warn!(client = %client_addr, tried = backends.len(), "no backend accepted the connection");
}

/// Where the service is on every member that can take work, this node
/// included if it can too.
///
/// Sorted, so the round-robin walks the same order from one connection to the
/// next. `HashMap` order would be different every time the table changed.
fn backends(node: &LocalNode, table: &MemberTable, backend_port: u16) -> Vec<SocketAddr> {
    let mut addrs: Vec<SocketAddr> = table
        .ready()
        .iter()
        .map(|member| SocketAddr::new(member.addr.ip(), backend_port))
        .collect();

    // we're not in our own table, but our own service is as good a backend as
    // anyone's. same rule as for the others: our advertised IP, the app's
    // port, and only while its check passes
    if node.is_ready() {
        addrs.push(SocketAddr::new(node.addr.ip(), backend_port));
    }

    addrs.sort();
    // two members on one IP (two agents on one machine) both map to the same
    // backend. without this it would get twice the traffic
    addrs.dedup();
    addrs
}
