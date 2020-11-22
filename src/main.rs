use clap::Clap;
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
        SubCommand::Groups(_) => rawslogs::list_groups().await,
        SubCommand::Streams(s) => rawslogs::list_streams(s.group).await,
        SubCommand::Get(g) => rawslogs::list_events(g.group, g.start, g.end).await,
    }
}
