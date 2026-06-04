//! Raw-TCP calculator built with [`stream_connect`] and [`stream_accept`].
//!
//! Shows the `stream` module end-to-end:
//!
//! - **Server** — `TcpListener::accept` → [`stream_accept`]`(tcp, None)` → `MaybeTlsStream`
//! - **Client** — [`stream_connect`]`(addr, None)` → `MaybeTlsStream` → split → read/write
//! - **Transparent TLS** — replace `None` with a [`TlsAcceptor`] / [`TlsConnector`] and the
//!   same `serve_connection` / `calc_op` helpers work unchanged.
//!
//! ## Protocol (newline-framed ASCII)
//!
//! ```text
//! → ADD 10.5 4.5\n         ← OK 15\n
//! → DIV 10.0 0.0\n         ← ERR division by zero\n
//! ```
//!
//! Run: `cargo run --example stream_calc`
//! See: `examples/stream_calc.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::{spawn, stream_accept, stream_connect, MaybeTlsStream};
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Protocol: evaluate one "OP A B" line
// ---------------------------------------------------------------------------

fn eval(line: &str) -> Result<f64, String> {
    let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
    if parts.len() != 3 {
        return Err(format!("expected OP A B, got {line:?}"));
    }
    let a: f64 = parts[1]
        .parse()
        .map_err(|_| format!("bad operand: {}", parts[1]))?;
    let b: f64 = parts[2]
        .parse()
        .map_err(|_| format!("bad operand: {}", parts[2]))?;
    match parts[0] {
        "ADD" => Ok(a + b),
        "SUB" => Ok(a - b),
        "MUL" => Ok(a * b),
        "DIV" if b == 0.0 => Err("division by zero".into()),
        "DIV" => Ok(a / b),
        op => Err(format!("unknown op: {op}")),
    }
}

// ---------------------------------------------------------------------------
// Server: one connection per tokio::spawn, driven via stream_accept
// ---------------------------------------------------------------------------

/// Reads lines from the `MaybeTlsStream`, evaluates each as "OP A B", replies
/// "OK result\n" or "ERR reason\n".  The stream is obtained from [`stream_accept`]
/// so this function is agnostic to whether TLS is active.
async fn serve_connection(peer: SocketAddr, stream: MaybeTlsStream) {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut lines = BufReader::new(read_half).lines();
    println!("[server] {peer} connected");

    while let Ok(Some(line)) = lines.next_line().await {
        let response = match eval(&line) {
            Ok(v) => format!("OK {v}\n"),
            Err(e) => format!("ERR {e}\n"),
        };
        println!("[server] {line:?}  →  {}", response.trim());
        if write_half.write_all(response.as_bytes()).await.is_err() {
            break;
        }
    }
    println!("[server] {peer} disconnected");
}

/// Accept loop: hands each incoming TCP connection to [`stream_accept`], then
/// dispatches it to [`serve_connection`] on a fresh task.
async fn accept_loop(listener: TcpListener) {
    loop {
        let Ok((tcp, peer)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            match stream_accept(tcp, None).await {
                Ok(stream) => serve_connection(peer, stream).await,
                Err(e) => tracing::error!(%peer, error = %e, "stream_accept failed"),
            }
        });
    }
}

// ---------------------------------------------------------------------------
// CalcServer actor — owns the TcpListener, launches the accept loop in pre_start
// ---------------------------------------------------------------------------

/// `ServerMsg` is uninhabited: the server actor has no runtime messages.
/// All work runs in the background accept task started by `pre_start`.
enum ServerMsg {}

struct CalcServer {
    listener: Option<TcpListener>,
    _accept_task: Option<JoinHandle<()>>,
}

impl CalcServer {
    fn with_listener(listener: TcpListener) -> Self {
        Self {
            listener: Some(listener),
            _accept_task: None,
        }
    }
}

#[async_trait::async_trait]
impl Actor<ServerMsg> for CalcServer {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let listener = self.listener.take().ok_or("listener already consumed")?;
        let addr = listener.local_addr()?;
        println!("[server] accept loop started on {addr}");
        self._accept_task = Some(tokio::spawn(accept_loop(listener)));
        Ok(())
    }

    async fn handle(&mut self, msg: ServerMsg) -> Result<(), ActorProcessingErr> {
        // ServerMsg is an empty enum — this branch is unreachable by construction.
        match msg {}
    }
}

// ---------------------------------------------------------------------------
// Client helper: stream_connect → split → one-shot request/response
// ---------------------------------------------------------------------------

/// Dials `addr` via [`stream_connect`], sends one `"OP A B\n"` line, and
/// reads back the server's `"OK v\n"` or `"ERR reason\n"` reply.
/// Passing a `TlsConnector` here is the only change needed for TLS clients.
async fn calc_op(addr: &str, op: &str, a: f64, b: f64) -> anyhow::Result<f64> {
    let stream = stream_connect(addr, None).await?;
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut lines = BufReader::new(read_half).lines();

    write_half
        .write_all(format!("{op} {a} {b}\n").as_bytes())
        .await?;

    match lines.next_line().await?.as_deref() {
        Some(r) if r.starts_with("OK ") => Ok(r[3..].trim().parse()?),
        Some(r) if r.starts_with("ERR ") => Err(anyhow::anyhow!("{}", &r[4..])),
        other => Err(anyhow::anyhow!("unexpected response: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("=== stream_calc — raw TCP calculator via MaybeTlsStream ===\n");

    // Bind first so we know the address before the actor starts.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();

    // Spawn the server actor with no supervisor (TcpListener is not Clone).
    // In production, use a factory that re-binds on the same port to get
    // supervisor restartability.
    let (_server_ref, _join) = spawn::<ServerMsg, _>(
        CalcServer::with_listener(listener),
        None, // no supervisor
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Brief settle for pre_start (accept loop) to be running.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    println!();

    // --- Sequential operations ---
    println!("--- Arithmetic ---");
    println!(
        "ADD 10.5  4.5  = {}",
        calc_op(&addr, "ADD", 10.5, 4.5).await?
    );
    println!(
        "SUB 100.0 37.5 = {}",
        calc_op(&addr, "SUB", 100.0, 37.5).await?
    );
    println!(
        "MUL   6.0  7.0 = {}",
        calc_op(&addr, "MUL", 6.0, 7.0).await?
    );
    println!(
        "DIV  22.0  7.0 = {:.6}",
        calc_op(&addr, "DIV", 22.0, 7.0).await?
    );

    // --- Error case ---
    println!("\n--- Error case ---");
    match calc_op(&addr, "DIV", 9.0, 0.0).await {
        Err(e) => println!("DIV 9.0 0.0 → ERR: {e}"),
        Ok(v) => println!("DIV 9.0 0.0 → unexpected OK {v}"),
    }

    // --- Concurrent connections ---
    // Each calc_op opens its own MaybeTlsStream; the server handles them in parallel.
    println!("\n--- Concurrent connections ---");
    let (a, b, c) = tokio::join!(
        calc_op(&addr, "ADD", 1.0, 2.0),
        calc_op(&addr, "MUL", 3.0, 4.0),
        calc_op(&addr, "SUB", 10.0, 5.0),
    );
    println!("1+2={}  3×4={}  10−5={}", a?, b?, c?);

    println!("\nDone. See examples/stream_calc.md for architecture details.");
    Ok(())
}
