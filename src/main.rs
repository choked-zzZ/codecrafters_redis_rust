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
                let mut temp = [0u8; 2048];
                let read_size = rd.read(&mut temp).await.expect("read stream error.");
                if read_size == 0 {
                    break;
                }
                buf.extend_from_slice(&temp[..read_size]);
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
                            buf.drain(..amt);
                        }
                    },
                }
            }
            println!("accepted new connection");
        });
    }
}
