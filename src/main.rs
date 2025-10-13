use clap::Parser;
use futures::lock;
use futures::lock::Mutex;
use futures::SinkExt;
use futures::StreamExt;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::redis_command::RedisCommand;
use crate::resp_decoder::RespParser;
use crate::resp_decoder::Value;
use env::Env;

mod env;
mod redis_command;
mod resp_decoder;

const REPL_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const REPL_OFFSET: &str = "0";

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

    while let Some(result) = framed.next().await {
        match result {
            Ok(value) => {
                eprintln!("address {addr} get recieved: {value:?}");
                let value = Arc::new(value);
                let command = RedisCommand::parse_command(value.clone());
                let response = command.clone().exec(env.clone(), addr, args.clone()).await;
                eprintln!("{addr} will send back: {response:?}");
                if let Err(e) = framed.send(&response).await {
                    eprintln!("carsh into error: {e}");
                    eprintln!("connection closed.");
                    return;
                }
                if matches!(command, RedisCommand::PSync(..)) {
                    break;
                }
                if command.can_modify() {
                    let mut env = env.lock().await;
                    if let Some(replicas) = env.replicas.get_mut(&addr) {
                        for replica in replicas.iter_mut() {
                            replica.send(&value).await.expect("send error.");
                            eprintln!("send a command");
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("failed to decode frame: {e:?}");
                eprintln!("connection closed.");
                return;
            }
        }
    }
    match env.lock().await.replicas.entry(addr) {
        Entry::Occupied(mut entry) => {
            entry.get_mut().push(framed);
        }
        Entry::Vacant(e) => {
            e.insert(vec![framed]);
        }
    }
    eprintln!("{addr}: replica mode on.")
}

async fn replica_handler(addr: String, args: &Arc<Args>, env: Arc<Mutex<Env>>) {
    let part = addr.split_ascii_whitespace().collect::<Vec<_>>();
    // let mut ip_str = part[0];
    let ip = [127u8, 0, 0, 1];
    let port = part[1].parse().unwrap();
    let addr = SocketAddr::new(std::net::IpAddr::from(ip), port);
    eprintln!("{addr} goes in replica handler");
    if let Ok(stream) = TcpStream::connect(addr).await {
        let mut framed = Framed::new(stream, RespParser);
        let handshake_first = Value::Array([Value::BulkString("PING".into())].into());
        framed.send(&handshake_first).await.unwrap();
        framed.next().await;
        let replconf_1 = Value::Array(
            [
                Value::BulkString("REPLCONF".into()),
                Value::BulkString("listening-port".into()),
                Value::BulkString(args.port.to_string().into()),
            ]
            .into(),
        );
        framed.send(&replconf_1).await.unwrap();
        framed.next().await;
        let replconf_2 = Value::Array(
            [
                Value::BulkString("REPLCONF".into()),
                Value::BulkString("capa".into()),
                Value::BulkString("psync2".into()),
            ]
            .into(),
        );
        framed.send(&replconf_2).await.unwrap();
        framed.next().await;
        let psync = Value::Array(
            [
                Value::BulkString("PSYNC".into()),
                Value::BulkString("?".into()),
                Value::BulkString("-1".into()),
            ]
            .into(),
        );
        framed.send(&psync).await.unwrap();
        framed.next().await;
        while let Some(result) = framed.next().await {
            match result {
                Ok(value) => {
                    eprintln!("got {value:?}");
                }
                Err(e) => {
                    eprintln!("get error {e:?}");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();
    let env = Arc::new(Mutex::new(Env::default()));
    let args_1 = args.clone();

    // Replica
    if let Some(addr) = &args.replicaof {
        // match env.lock().await.replicas.entry(socket_addr) {
        //     Entry::Occupied(mut entry) => {
        //         entry.get_mut().push(value);
        //     }
        // }
        let addr = addr.clone();
        let env = env.clone();
        tokio::spawn(async move {
            replica_handler(addr, &args_1, env).await;
        });
    }

    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .expect("listener connection failed.");
        eprintln!("New connection from {addr}");
        env.lock().await.replicas.insert(addr, Vec::new());

        let env = env.clone();
        let args = args.clone();
        tokio::spawn(async move {
            connection_handler(stream, env, addr, args).await;
        });
    }
}
