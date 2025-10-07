use bytes::Bytes;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::resp_decoder::{RespParser, Value};

pub enum RedisCommand {
    Ping,
    Echo(Value),
}

impl RedisCommand {
    pub async fn exec(
        self,
        framed: &mut Framed<TcpStream, RespParser>,
    ) -> Result<(), std::io::Error> {
        match self {
            RedisCommand::Ping => {
                let response = Value::String(Bytes::from("PONG"));
                framed.send(response).await
            }
            RedisCommand::Echo(msg) => framed.send(msg).await,
        }
    }
    pub fn parse_command(value: Value) -> RedisCommand {
        match value {
            Value::String(s) if s == "PING" => RedisCommand::Ping,
            Value::Array(arr) if !arr.is_empty() => {
                let command = arr.first().unwrap();
                match command {
                    Value::BulkString(s) => {
                        assert!(s == "ECHO");
                        assert!(arr.len() == 2);
                        RedisCommand::Echo(arr.get(1).unwrap().clone())
                    }
                    _ => panic!("Unknown command or invalid arguments"),
                }
            }
            _ => panic!("Unknown command"),
        }
    }
}
