use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::redis_command::RedisCommand;
use crate::resp_decoder::RespParser;

mod redis_command;
mod resp_decoder;

async fn connection_handler(stream: TcpStream) {
    let mut framed = Framed::new(stream, RespParser);

    while let Some(result) = framed.next().await {
        match result {
            Ok(value) => {
                eprintln!("recieved: {value:?}");
                if let Err(e) = RedisCommand::parse_command(value).exec(&mut framed).await {
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
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .expect("listener connection failed.");
        eprintln!("New connection from {addr}");

        tokio::spawn(async move {
            connection_handler(stream).await;
        });
    }
}
