use clap::Clap;
use rusoto_core::Region;
use rusoto_logs::{CloudWatchLogs, CloudWatchLogsClient, DescribeLogGroupsRequest};
use std::env;

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
        SubCommand::Groups(_) => {
            let client = CloudWatchLogsClient::new(Region::default());
            match client
                .describe_log_groups(DescribeLogGroupsRequest {
                    limit: None,
                    log_group_name_prefix: None,
                    next_token: None,
                })
                .await
            {
                Ok(result) => {
                    if let Some(groups) = result.log_groups {
                        for group in &groups {
                            println!("{}", group.log_group_name.as_ref().unwrap())
                        }
                    }
                }
                Err(error) => println!("Error: {:?}", error),
            }
        }
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
