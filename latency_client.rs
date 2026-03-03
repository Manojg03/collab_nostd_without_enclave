use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    let url = url::Url::parse("ws://127.0.0.1:5560/collaboration/latency").unwrap();

    println!("connecting to {}", url);
    let (ws_stream, _) = match tokio_tungstenite::connect_async(url).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connect error: {}", e);
            return;
        }
    };

    println!("connected");
    let (mut write, mut read) = ws_stream.split();

    let iterations = 50u32;
    for i in 0..iterations {
        let payload = vec![b'a'; 1];
        let t0 = Instant::now();
        if let Err(e) = write.send(tokio_tungstenite::tungstenite::Message::Binary(payload)).await {
            eprintln!("send error: {}", e);
            break;
        }

        // wait for any reply (with timeout)
        let recv = tokio::time::timeout(Duration::from_secs(2), read.next()).await;
        match recv {
            Ok(Some(Ok(msg))) => {
                let t1 = Instant::now();
                println!("iter {} got reply kind={:?} latency={:?}", i, msg, t1.duration_since(t0));
            }
            Ok(Some(Err(e))) => {
                eprintln!("read error: {}", e);
                break;
            }
            Ok(None) => {
                println!("iter {}: stream ended", i);
                break;
            }
            Err(_) => {
                println!("iter {}: timeout waiting for reply", i);
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    println!("done");
}
