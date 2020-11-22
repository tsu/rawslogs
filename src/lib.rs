use http::StatusCode;
use rusoto_core::request::BufferedHttpResponse;
use rusoto_core::{Region, RusotoError};
use rusoto_logs::{
    CloudWatchLogs, CloudWatchLogsClient, DescribeLogGroupsError, DescribeLogGroupsRequest,
    DescribeLogGroupsResponse, DescribeLogStreamsError, DescribeLogStreamsRequest,
    DescribeLogStreamsResponse, LogGroup, LogStream,
};
use std::{thread, time};

const LIMIT: i64 = 50;

pub async fn list_groups() {
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

pub async fn list_streams(log_group_name: String) {
    let client = CloudWatchLogsClient::new(Region::default());
    let mut streams = Vec::<LogStream>::new();

    match describe_log_streams(&client, &log_group_name, None).await {
        Ok(DescribeLogStreamsResponse {
            log_streams,
            next_token,
        }) => {
            if let Some(mut log_streams) = log_streams {
                streams.append(&mut log_streams);
            }
            let mut token = next_token;
            while token.is_some() {
                match describe_log_streams(&client, &log_group_name, token.clone()).await {
                    Ok(DescribeLogStreamsResponse {
                        log_streams,
                        next_token,
                    }) => {
                        if let Some(mut log_streams) = log_streams {
                            streams.append(&mut log_streams);
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

    for g in streams {
        println!("{}", g.log_stream_name.expect("mf"));
    }
}

pub async fn list_events(log_group_name: String, start: String, end: Option<String>) {
    println!("get events for group {} after {}", log_group_name, start);
    if let Some(end) = end {
        println!("and before {}", end);
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

async fn describe_log_streams(
    client: &CloudWatchLogsClient,
    log_group_name: &String,
    next_token: Option<String>,
) -> Result<DescribeLogStreamsResponse, RusotoError<DescribeLogStreamsError>> {
    client
        .describe_log_streams(DescribeLogStreamsRequest {
            descending: Some(false),
            limit: Some(LIMIT),
            log_group_name: log_group_name.clone(),
            log_stream_name_prefix: None,
            next_token: next_token,
            order_by: Some("LastEventTime".to_string()),
        })
        .await
}

fn throttle() {
    thread::sleep(time::Duration::from_millis(100));
}
