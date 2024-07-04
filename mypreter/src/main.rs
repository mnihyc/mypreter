use std::{io::Write, sync::Arc};

use proto::comm::CommListener;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    //cmd: String,
    #[arg(default_value_t = String::from("0.0.0.0:9876"))]
    hostr: String,
}

#[tokio::main]
async fn main() {
    //std::env::set_var("RUST_LOG", "debug");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let manager = Arc::new(proto::session::SessionManager::new());
    let encrypt = Arc::new(proto::encrypt::enc_plain::EncryptPlain::new());
    let param = proto::comm::comm_tcp::TcpContextParam::new(Some(1), Some(false));
    let context = proto::comm::comm_tcp::TcpContext::new(encrypt.clone(), false, manager.clone(), param);
    let listener = proto::comm::comm_tcp::CommTcpListener::new(args.hostr, context);
    loop {
        let session = listener.bind().await.unwrap().unwrap();
        let channel = proto::channel::Channel::new(session.clone(), encrypt.clone(), 8);
        loop {
            let mut cmd = String::new();
            std::io::stdout().write_all(b"shell> ").unwrap();
            std::io::stdout().flush().unwrap();
            std::io::stdin().read_line(&mut cmd).unwrap();
            if cmd.trim() == "exit" {
                manager.close_session(session.get_id()).await;
                break;
            }
            let packet = proto::packet::Packet::new(proto::packet::PacketPayload::CmdExec(proto::packet::CmdExec { cmd: cmd.clone() }));
            match channel.send(packet).await {
                Ok(_) => log::debug!("Sent command: {}", cmd),
                Err(e) => {
                    log::warn!("Send error: {}", e);
                    break;
                }
            };
            while let Ok(packet) = channel.recv().await {
                match packet.payload {
                    proto::packet::PacketPayload::CmdExecRes(info) => {
                        log::info!("Received result: {}", info.result);
                        break;
                    },
                    _ => {
                        log::warn!("Received unexpected packet: {:?}", packet);
                    }
                }
            }
        }
    }
}
