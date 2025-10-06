use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (mut stream, addr) = listener
            .accept()
            .await
            .expect("listener connection failed.");
        eprintln!("New connection from {addr}");

        tokio::spawn(async move {
            let (mut rd, mut wt) = stream.split();

            let mut buf = Vec::new();
            loop {
                sleep(Duration::from_secs(2)).await;
                if rd.read(&mut buf).await.expect("read stream error.") == 0 {
                    break;
                }
                match redis_protocol::resp2::decode::decode(&buf) {
                    Err(e) => {
                        eprintln!("{e}");
                        break;
                    }
                    Ok(res) => match res {
                        None => eprintln!("Incomplete frame."),
                        Some((val, amt)) => {
                            eprintln!("parsed {amt} byte(s) and get {val:?}");
                            wt.write_all(b"+PONG\r\n").await.expect("write error.");
                        }
                    },
                }
            }
            println!("accepted new connection");
        });
    }
}
