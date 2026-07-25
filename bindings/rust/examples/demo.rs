// Demo for the Rust binding, in two roles:
//
//   cargo run --example demo -- listen     subscribe to demo.chat, answer demo.echo
//   cargo run --example demo -- send        publish on demo.chat, then request demo.echo
//
// Start a broker (./build/server/wisp-broker) and a listener first, then send.
// Two processes are required: the broker never routes a message back to its
// own sender. Build/run with the C ABI on the library path, e.g.
//
//   WISP_LIB_DIR=../../build/common LD_LIBRARY_PATH=../../build/common \
//     cargo run --example demo -- listen

use std::time::Duration;

use wisp::ConnectionConfig;

const BROKER: &str = "tcp://127.0.0.1:5555";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let role = std::env::args().nth(1).unwrap_or_default();
    wisp::set_log_level(wisp::LogLevel::Warning);

    match role.as_str() {
        "listen" => {
            wisp::init_connection(ConnectionConfig { address: BROKER, client_id: Some("rust-listener"), ..Default::default() })?;
            wisp::wait_for_connection(5000)?;
            let _chat = wisp::register_callback("demo.chat", |topic, data| {
                println!("[{topic}] {}", String::from_utf8_lossy(data));
            })?;
            let _echo = wisp::register_callback("demo.echo", |_topic, data| {
                let mut reply = b"echo: ".to_vec();
                reply.extend_from_slice(data);
                let _ = wisp::reply_to_sender(&reply);
            })?;
            println!("listening on demo.chat / demo.echo (Ctrl-C to stop)");
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        "send" => {
            wisp::init_connection(ConnectionConfig { address: BROKER, client_id: Some("rust-sender"), ..Default::default() })?;
            wisp::wait_for_connection(5000)?;
            wisp::send_message("demo.chat", "hello from Rust")?;
            let reply = wisp::send_request("demo.echo", b"ping", 2000, 256)?;
            println!("request answered: {}", String::from_utf8_lossy(&reply));
            wisp::shutdown_connection();
            Ok(())
        }
        _ => {
            eprintln!("usage: demo <listen|send>");
            std::process::exit(2);
        }
    }
}
