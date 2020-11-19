use clap::Clap;
use http::StatusCode;
use rusoto_core::request::BufferedHttpResponse;
use rusoto_core::{Region, RusotoError};
use rusoto_logs::{
    CloudWatchLogs, CloudWatchLogsClient, DescribeLogGroupsError, DescribeLogGroupsRequest,
    DescribeLogGroupsResponse, LogGroup,
};
use std::env;
use std::{thread, time};

const LIMIT: i64 = 1;

#[derive(Clap)]
#[clap(version = "0.1.0", author = "Timo Suomela <timo.suomela@reaktor.com>")]
struct Opts {
    /// The aws profile to use
    #[clap(short, long)]
    profile: Option<String>,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    Groups(Groups),
    Streams(Streams),
    Get(Get),
}

/// List cloudwatch log groups
#[derive(Clap)]
struct Groups {}

/// List cloudwatch log streams
#[derive(Clap)]
struct Streams {
    /// The name of the the aws log group
    group: String,
}

/// Get the log events of an aws log group
#[derive(Clap)]
struct Get {
    /// The name of the the aws log group
    group: String,
    /// Only show events that are newer than 'start'
    #[clap(short, long, default_value = "1 hour ago")]
    start: String,
    /// Only show events that are older than 'end'
    #[clap(short, long)]
    end: Option<String>,
}

#[tokio::main]
async fn main() {
    let opts: Opts = Opts::parse();
    if let Some(p) = opts.profile {
        env::set_var("AWS_PROFILE", p);
    }
    match opts.subcmd {
        SubCommand::Groups(_) => list_groups().await,
        SubCommand::Streams(s) => println!("get streams for group {}", s.group),
        SubCommand::Get(g) => {
            println!("get events for group {} after {}", g.group, g.start);
            match g.end {
                Some(end) => println!("and before {}", end),
                _ => {}
            }
        }
    }
}

async fn list_groups() {
    let client = CloudWatchLogsClient::new(Region::default());
    let mut groups = Vec::<LogGroup>::new();

    match describe_log_groups(&client, None).await {
        Ok(DescribeLogGroupsResponse {
            log_groups,
            next_token,
        }) => {
            if let Some(mut log_groups) = log_groups {
                groups.append(&mut log_groups);
            }
            let mut token = next_token;
            while token.is_some() {
                match describe_log_groups(&client, token.clone()).await {
                    Ok(DescribeLogGroupsResponse {
                        log_groups,
                        next_token,
                    }) => {
                        if let Some(mut log_groups) = log_groups {
                            groups.append(&mut log_groups);
                        }
                        token = next_token;
                    }
                    Err(RusotoError::Unknown(BufferedHttpResponse {
                        status: StatusCode::BAD_REQUEST,
                        body: _,
                        headers: _,
                    })) => throttle(),
                    Err(error) => eprintln!("Error: {:?}", error),
                }
            }
        }
        Err(error) => eprintln!("Error: {:?}", error),
    }

    for g in groups {
        println!("{}", g.log_group_name.expect("mf"));
    }
}

async fn describe_log_groups(
    client: &CloudWatchLogsClient,
    next_token: Option<String>,
) -> Result<DescribeLogGroupsResponse, RusotoError<DescribeLogGroupsError>> {
    client
        .describe_log_groups(DescribeLogGroupsRequest {
            limit: Some(LIMIT),
            log_group_name_prefix: None,
            next_token: next_token,
        })
        .await
}

fn throttle() {
    thread::sleep(time::Duration::from_millis(100));
}
