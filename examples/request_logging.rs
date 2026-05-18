#![cfg(not(target_arch = "wasm32"))]

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        read_request(&mut socket, &mut request).await;

        let body = "hello from the local server\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    let client = reqwest::Client::builder()
        .request_logger(reqwest::logging::Logger::new())
        .build()?;

    let response_text = client
        .post(format!("http://{addr}/demo?show=request-logger"))
        .header(reqwest::header::CONTENT_TYPE, "text/plain")
        .body("this request body is not logged")
        .send()
        .await?
        .text()
        .await?;

    println!("response text: {response_text}");
    server.await?;
    Ok(())
}

async fn read_request(socket: &mut tokio::net::TcpStream, request: &mut Vec<u8>) {
    let mut buffer = [0; 1024];
    let headers_end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        if n == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..n]);
        if let Some(index) = find_bytes(request, b"\r\n\r\n") {
            break index + 4;
        }
    };

    let content_length = content_length(&request[..headers_end]);
    while request.len() < headers_end + content_length {
        let n = socket.read(&mut buffer).await.unwrap();
        if n == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..n]);
    }
}

fn content_length(headers: &[u8]) -> usize {
    std::str::from_utf8(headers)
        .ok()
        .and_then(|headers| {
            headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())
                    .flatten()
            })
        })
        .unwrap_or(0)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
