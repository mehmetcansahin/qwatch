extern crate env_logger;
extern crate rusoto_core;
extern crate rusoto_sqs;

use rusoto_core::Region;
use rusoto_sqs::{
    DeleteMessageRequest, Message, ReceiveMessageRequest, SendMessageRequest, Sqs, SqsClient,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;

use std::env;
use std::fs;
use std::io::{self, Write};
use std::process::Command;

#[derive(Deserialize, Clone, Debug)]
struct Config {
    aws_access_key_id: String,
    aws_secret_access_key: String,
    queue_url: String,
    region: String,
    failed_queue_url: String,
    commands: Vec<Cmd>,
}

#[derive(Deserialize, Clone, Debug)]
struct Cmd {
    name: String,
    program: String,
    args: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct QueueCommand {
    name: String,
    args: Vec<String>,
}

fn init_sqs(config: Config) -> SqsClient {
    if env::var("AWS_ACCESS_KEY_ID").is_err() {
        env::set_var("AWS_ACCESS_KEY_ID", config.aws_access_key_id);
    }
    if env::var("AWS_SECRET_ACCESS_KEY").is_err() {
        env::set_var("AWS_SECRET_ACCESS_KEY", config.aws_secret_access_key);
    }
    let region = Region::from_str(&config.region.to_owned()).expect("Region problem");
    return SqsClient::new(region);
}

fn load_config() -> Config {
    let config_string =
        fs::read_to_string("config.json").expect("Something went wrong reading the file");
    return serde_json::from_str(&config_string.to_owned()).expect("config.json error");
}

async fn failed_job(sqs: SqsClient, failed_queue_url: String, cmd: QueueCommand) {
    let data = json!(cmd);
    let msg_str = data.to_string();
    let send_msg_request = SendMessageRequest {
        message_body: msg_str.clone(),
        queue_url: failed_queue_url.clone(),
        ..Default::default()
    };
    let _response = sqs.send_message(send_msg_request).await;
}

async fn delete_message(sqs: SqsClient, config: Config, msg: Message) {
    let delete_message_request = DeleteMessageRequest {
        queue_url: config.queue_url.clone(),
        receipt_handle: msg.receipt_handle.clone().unwrap(),
    };
    match sqs.delete_message(delete_message_request).await {
        Ok(_) => println!(
            "Deleted message via receipt handle {:?}",
            msg.receipt_handle
        ),
        Err(e) => panic!("Couldn't delete message: {:?}", e),
    }
}

async fn run_job(sqs: SqsClient, config: Config, cmd: QueueCommand, msg_cmd: &Cmd) {
    let args = msg_cmd
        .args
        .iter()
        .filter(|&x| cmd.args.iter().find(|&y| y.eq(x)).is_some())
        .collect::<Vec<&String>>();
    let output = Command::new(msg_cmd.program.clone()).args(&args).output();
    match output {
        Ok(output) => {
            io::stdout().write_all(&output.stdout).unwrap();
        }
        Err(_err) => {
            failed_job(sqs.clone(), config.failed_queue_url.clone(), cmd.clone()).await;
        }
    }
}

async fn rsp_proc(sqs: SqsClient, config: Config, rsp: Vec<Message>) {
    for msg in rsp {
        delete_message(sqs.clone(), config.clone(), msg.clone()).await;
        let cmd: QueueCommand = serde_json::from_str(&msg.body.unwrap().to_string()).unwrap();
        let msg_cmd = config
            .commands
            .iter()
            .find(|msg_cmd| msg_cmd.name == cmd.name);
        match msg_cmd {
            Some(msg_cmd) => run_job(sqs.clone(), config.clone(), cmd.clone(), msg_cmd).await,
            None => {}
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = env_logger::try_init();
    let config = load_config();
    let sqs = init_sqs(config.clone());
    let receive_request = ReceiveMessageRequest {
        queue_url: config.queue_url.to_owned(),
        message_attribute_names: Some(vec!["All".to_string()]),
        ..Default::default()
    };
    loop {
        let response = sqs.receive_message(receive_request.clone()).await;
        match response
            .expect("Expected to have a receive message response")
            .messages
        {
            Some(rsp) => rsp_proc(sqs.clone(), config.clone(), rsp).await,
            None => {}
        };
    }
}
