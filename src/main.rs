use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::Parser;
use futures::stream::SplitSink;
use futures::SinkExt;
use futures::StreamExt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::codec::Framed;

use crate::redis_command::RedisCommand;
use crate::resp_decoder::RespParser;
use crate::resp_decoder::Value;
use env::Env;

mod env;
mod rdb_reader;
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

    #[arg(long)]
    dir: Option<String>,

    #[arg(long)]
    dbfilename: Option<String>,
}

async fn connection_handler(
    stream: TcpStream,
    env: Arc<Mutex<Env>>,
    addr: SocketAddr,
    args: Arc<Args>,
) {
    let framed = Framed::new(stream, RespParser);
    let framed = Arc::new(Mutex::new(framed));

    while let Some(result) = framed.lock().await.next().await {
        match result {
            Ok(value) => {
                eprintln!("address {addr} get recieved: {value:?}");
                let value = Arc::new(value);
                let command = RedisCommand::parse_command(value.clone());
                let response = command
                    .clone()
                    .exec(env.clone(), addr, args.clone(), framed.clone())
                    .await;
                eprintln!("{addr} will send back: {response:?}");
                if let Err(e) = framed.lock().await.send(&response).await {
                    eprintln!("carsh into error: {e}");
                    eprintln!("connection closed.");
                    return;
                }
                let mut env = env.lock().await;
                if matches!(command, RedisCommand::PSync(..)) {
                    env.ack -= 14 + 48 + 40;
                    eprintln!("handshake over, ack now drop to {}", env.ack);
                    break;
                }
                if command.can_modify() {
                    for replica_framed in env.replicas.iter_mut() {
                        replica_framed
                            .lock()
                            .await
                            .send(&value)
                            .await
                            .expect("send error.");
                        eprintln!("send a command {command:?} to one replica");
                    }
                    eprintln!("command {command:?} send completed");
                }
                if !matches!(command, RedisCommand::Wait(..)) {
                    env.ack += value.buf_size();
                    eprintln!(
                        "command {command:?} contribute size {} and now comes to {}",
                        value.buf_size(),
                        env.ack
                    );
                }
            }
            Err(e) => {
                eprintln!("failed to decode frame: {e:?}");
                eprintln!("connection closed.");
                return;
            }
        }
    }

    env.lock().await.replicas.push(framed.clone());
    eprintln!("{addr}: replica mode on.")
}

async fn replica_handler(addr: String, args: &Arc<Args>, env: Arc<Mutex<Env>>) {
    let part = addr.split_ascii_whitespace().collect::<Vec<_>>();
    // let mut ip_str = part[0];
    let ip = [127u8, 0, 0, 1];
    let port = part[1].parse().unwrap();
    let addr = SocketAddr::new(std::net::IpAddr::from(ip), port);
    eprintln!("127.0.0.1:{} goes in replica handler", args.port);
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
        framed.next().await;
        let framed = Arc::new(Mutex::new(framed));
        while let Some(result) = framed.lock().await.next().await {
            match result {
                Ok(value) => {
                    eprintln!("got {value:?}");
                    let value = Arc::new(value);
                    let command = RedisCommand::parse_command(value.clone());
                    let response = command
                        .clone()
                        .exec(env.clone(), addr, args.clone(), framed.clone())
                        .await;
                    env.lock().await.ack += value.buf_size();
                    eprintln!(
                        "127.0.0.1:{} increse ack with {} so it becomes {}",
                        args.port,
                        value.buf_size(),
                        env.lock().await.ack
                    );
                    if matches!(command, RedisCommand::Replconf(..)) {
                        eprintln!("{addr} will send back: {response:?}");
                        if let Err(e) = framed.lock().await.send(&response).await {
                            eprintln!("carsh into error: {e}");
                            eprintln!("connection closed.");
                            return;
                        }
                    }
                    if matches!(command, RedisCommand::PSync(..)) {
                        break;
                    }
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
    let mut env = Env::default();
    let args_1 = args.clone();

    env.file_path = args
        .dir
        .as_ref()
        .zip(args.dbfilename.as_ref())
        .map(|(dir, filename)| Path::new(dir).join(filename).into_boxed_path());

    if matches!(env.file_path.as_ref().map(|x| x.exists()), Some(false)) {
        eprintln!("file not exist");
        tokio::fs::write(env.file_path.as_ref().unwrap(), STANDARD.decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==").unwrap()).await.unwrap();
    }
    if env.file_path.is_some() {
        let (map, expiry) = rdb_reader::rbd_reader(env.file_path.as_ref().unwrap().as_ref()).await;
        env.map = map;
        env.expiry = expiry;
    }

    let env = Arc::new(Mutex::new(env));

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

        let env = env.clone();
        let args = args.clone();
        tokio::spawn(async move {
            connection_handler(stream, env, addr, args).await;
        });
    }
}
