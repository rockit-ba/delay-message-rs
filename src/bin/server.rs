use delay_message_rs::commit_log;
use delay_message_rs::consume_queue;
use delay_message_rs::log_util::log_init;
use delay_message_rs::message::Message;
use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    log_init();
    // 开始初始化延迟消息
    info!("开始初始化延迟消息-->");
    consume_queue::init().await;

    let commit_log_rx = commit_log::mpsc_channel();
    let first = commit_log_rx.clone();
    tokio::spawn(async move {
        let json = String::from("{\"msg_len\":66,\"body_crc\":342342,\"physical_offset\":0,\"send_timestamp\":1232432443,\"store_timestamp\":1232432999,\"body_len\":21,\"body\":\"此情可待成追忆\",\"topic_len\":9,\"topic\":\"topic_oms\",\"prop_len\":0,\"prop\":\"\"}");
        let message = Message::deserialize_json(&json);
        first.send(message).expect("消息发送通道失败");
    });

    info!("开始监听-->");
    let listener = TcpListener::bind("127.0.0.1:9999").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {e}");
                        return;
                    }
                };

                // Write the data back
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    eprintln!("failed to write to socket; err = {e}");
                    return;
                }
            }
        });
    }
}
