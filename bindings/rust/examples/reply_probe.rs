//! Exercises the non-blocking request/reply pair against a live broker:
//!
//! ```sh
//! WISP_LIB_DIR=../../build/common LD_LIBRARY_PATH=../../build/common \
//!   cargo run --example reply_probe
//! ```
//!
//! Start a broker (tcp://127.0.0.1:25999, or `$WISP_BROKER`) and something
//! answering `svc/echo` first - a broker never routes a message back to its own
//! sender, so the responder has to be a separate process.
use std::sync::mpsc;
use std::time::Duration;

fn main() -> wisp::Result<()> {
    let broker = std::env::var("WISP_BROKER").unwrap_or_else(|_| "tcp://127.0.0.1:25999".to_string());

    wisp::set_log_level(wisp::LogLevel::Error);
    wisp::init_connection(wisp::ConnectionConfig { address: &broker, client_id: Some("rust-asker"), ..Default::default() })?;
    wisp::wait_for_connection(3000)?;

    let reply_topic = wisp::make_reply_topic("svc/echo")?;
    assert!(reply_topic.starts_with("svc/echo"), "{reply_topic}");
    assert!(!reply_topic.contains('\0'), "reply topic carries a trailing NUL");
    assert_ne!(reply_topic, wisp::make_reply_topic("svc/echo")?, "reply topics must be unique");
    println!("make_reply_topic: {reply_topic}");

    // A reserved reply topic is refused rather than silently lost: a broker
    // drops "__" keys instead of routing them.
    match wisp::send_data_with_reply("svc/echo", b"x", "__nope__") {
        Ok(()) => println!("FAIL: a reserved reply topic was accepted"),
        Err(e) => println!("reserved reply topic refused: {}", e.message),
    }

    let (tx, rx) = mpsc::channel();
    let _reply = wisp::register_callback(&reply_topic, move |_topic, data| {
        let _ = tx.send(String::from_utf8_lossy(data).into_owned());
    })?;

    wisp::send_data_with_reply("svc/echo", b"hello from rust", &reply_topic)?;

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(answer) => println!("answer: {answer}"),
        Err(_) => println!("answer: (none)"),
    }

    wisp::shutdown_connection();
    Ok(())
}
