use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((mut stream, _)) => {
                println!("Accepted new connection");
                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    loop {
                        match stream.read(&mut buffer).await {
                            Ok(n) => {
                                if n == 0 {
                                    break;
                                }
                                let input_str = String::from_utf8_lossy(&buffer[..n]);
                                let input_str = input_str.trim();
                                println!("received input: {}", input_str);
                                let commands: Vec<_> =
                                    input_str.split_terminator("\\r\\n").collect();
                                for _ in commands.iter().filter(|cmd| cmd.contains("PING")) {
                                    if let Err(e) = stream.write(b"+PONG\r\n").await {
                                        eprintln!("Error writing to stream: {}", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error reading from stream: {}", e);
                            }
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
                break;
            }
        }
    }
}
