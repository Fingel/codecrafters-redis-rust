use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                loop {
                    let mut buffer = [0; 1024];
                    match stream.read(&mut buffer) {
                        Ok(n) => {
                            if n == 0 {
                                break;
                            }
                            let input_str = String::from_utf8_lossy(&buffer[..n]);
                            let input_str = input_str.trim();
                            println!("received input: {}", input_str);
                            let commands: Vec<_> = input_str.split_terminator("\\r\\n").collect();
                            commands
                                .iter()
                                .filter(|cmd| cmd.contains("PING"))
                                .for_each(|_| {
                                    write!(stream, "+PONG\r\n").unwrap();
                                });
                        }
                        Err(e) => {
                            println!("error: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
