use clap::Parser;
use futures::lock::Mutex;
use futures::SinkExt;
use futures::StreamExt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::redis_command::RedisCommand;
use crate::resp_decoder::RespParser;
use crate::resp_decoder::Value;
use env::Env;

mod env;
mod redis_command;
mod resp_decoder;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, short, default_value_t = 6379)]
    port: u32,

    #[arg(long, short)]
    replicaof: Option<String>,
}

async fn connection_handler(
    stream: TcpStream,
    env: Arc<Mutex<Env>>,
    addr: SocketAddr,
    args: Arc<Args>,
) {
    let mut framed = Framed::new(stream, RespParser);

    eprintln!("enter1.");
    if let Some(ref _addr) = args.replicaof {
        eprintln!("enter2.");
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        eprintln!("send.");
        let handshake_stream = TcpStream::connect(addr).await.expect("Connect failed.");
        let mut handshake_framed = Framed::new(handshake_stream, RespParser);
        eprintln!("hs1");
        handshake_framed
            .send(&Value::Array([Value::BulkString("PING".into())].into()))
            .await
            .expect("send failed.");
        eprintln!("suc");
    }

    while let Some(result) = framed.next().await {
        match result {
            Ok(value) => {
                eprintln!("recieved: {value:?}");
                let response = RedisCommand::parse_command(value)
                    .exec(env.clone(), addr, args.clone())
                    .await;
                eprintln!("{response:?}");
                if let Err(e) = framed.send(&response).await {
                    eprintln!("carsh into error: {e}");
                    break;
                }
            }
            Err(e) => {
                eprintln!("failed to decode frame: {e:?}");
                break;
            }
        }
    }
    eprintln!("connection closed.")
}

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();
    eprintln!("1");
    let env = Arc::new(Mutex::new(Env::default()));
    let args = args.clone();
    if let Some(addr) = &args.replicaof {
        let addr = addr.clone();
        tokio::spawn(async move {
            let part = addr.split_ascii_whitespace().collect::<Vec<_>>();
            let mut ip = part[0];
            if ip == "localhost" {
                ip = "127.0.0.1";
            }
            let port = part[1].parse().unwrap();
            if let Ok(stream) = TcpStream::connect((ip, port)).await {
                let mut framed = Framed::new(stream, RespParser);
                let handshake_first = Value::Array([Value::BulkString("PING".into())].into());
                framed.send(&handshake_first).await.unwrap();
            }
        });
    }
    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .expect("listener connection failed.");
        eprintln!("New connection from {addr}");

        let env = env.clone();
        let args = args.clone();
        tokio::spawn(async move {
            connection_handler(stream, env, addr, args).await;
        });
    }
}
