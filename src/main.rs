use chrono::{NaiveDateTime, Utc};
use clap::Clap;
use ms_converter::ms;
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
    Events(Events),
}

/// List log groups
#[derive(Clap)]
struct Groups {}

/// List log streams of a log group
#[derive(Clap)]
struct Streams {
    /// The name of the log group
    group: String,
}

/// List log events of a log group
#[derive(Clap)]
struct Events {
    /// The name of the the log group
    group: String,
    /// Only show events that are newer than 'start'
    #[clap(short, long, default_value = "1 hour ago")]
    start: String,
    /// Only show events that are older than 'end'
    #[clap(short, long)]
    end: Option<String>,
}

const ONE_HOUR_IN_SECONDS: i64 = 60 * 60;

#[tokio::main]
async fn main() {
    let opts: Opts = Opts::parse();
    if let Some(p) = opts.profile {
        env::set_var("AWS_PROFILE", p);
    }
    match opts.subcmd {
        SubCommand::Groups(_) => rawslogs::list_groups().await,
        SubCommand::Streams(s) => rawslogs::list_streams(s.group).await,
        SubCommand::Events(g) => {
            let now = Utc::now().timestamp();
            let start = now
                - match ms(g.start) {
                    Ok(start) => start / 1000,
                    Err(_) => ONE_HOUR_IN_SECONDS,
                };
            let end = now
                - g.end
                    .map(|end| match ms(end) {
                        Ok(end) => end / 1000,
                        Err(_) => 0,
                    })
                    .unwrap_or(0);

            println!(
                "Listing events between {} and {} and now is {}",
                NaiveDateTime::from_timestamp(start, 0),
                NaiveDateTime::from_timestamp(end, 0),
                NaiveDateTime::from_timestamp(now, 0)
            );
            rawslogs::list_events(&g.group, start * 1000, end * 1000).await
        }
    }
}
