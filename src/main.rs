use std::io::{BufReader, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut decoder =
                    resp::Decoder::new(BufReader::new(stream.try_clone().expect("Clone failed")));
                loop {
                    match decoder.decode() {
                        Err(e) => {
                            eprintln!("{e}");
                            break;
                        }
                        Ok(res) => {
                            eprintln!("{res:?}");
                            stream.write_all(b"+PONG\r\n").unwrap();
                        }
                    }
                }
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
