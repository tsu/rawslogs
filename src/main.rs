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
    /// Only show events that are newer than 'start'. Defaults to '1 hour'
    #[clap(short, long)]
    start: Option<String>,
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
            let params = rawslogs::ListGroupsParamsBuilder::new().build();
            rawslogs::list_groups(params).await
        }
        SubCommand::Streams(s) => {
            let params = rawslogs::ListStreamsParamsBuilder::new(s.group).build();
            rawslogs::list_streams(params).await
        }
        SubCommand::Events(g) => {
            let params = rawslogs::ListEventsParamsBuilder::new(g.group, g.start, g.end).build();
            rawslogs::list_events(params).await
        }
    }
}
