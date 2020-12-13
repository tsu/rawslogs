use chrono::{NaiveDateTime, Utc};
use http::StatusCode;
use ms_converter::ms;
use rusoto_core::request::BufferedHttpResponse;
use rusoto_core::{Region, RusotoError};
use rusoto_logs::{
    CloudWatchLogs, CloudWatchLogsClient, DescribeLogGroupsError, DescribeLogGroupsRequest,
    DescribeLogGroupsResponse, DescribeLogStreamsError, DescribeLogStreamsRequest,
    DescribeLogStreamsResponse, GetLogEventsError, GetLogEventsRequest, GetLogEventsResponse,
    LogGroup, LogStream, OutputLogEvent,
};
use std::{thread, time};

const LIMIT: i64 = 50;
const ONE_HOUR_IN_SECONDS: i64 = 60 * 60;

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
    for g in get_streams(&log_group_name).await {
        println!("{}", g.log_stream_name.expect("mf"));
    }
}

async fn get_streams(log_group_name: &String) -> Vec<LogStream> {
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

    return streams;
}

pub async fn list_events(log_group_name: &String, start: Option<String>, end: Option<String>) {
    let now = Utc::now().timestamp();
    let start_time = now
        - start
            .map(|start| match ms(start) {
                Ok(start) => start / 1000,
                Err(_) => ONE_HOUR_IN_SECONDS,
            })
            .unwrap_or(ONE_HOUR_IN_SECONDS);
    let end_time = now
        - end
            .map(|end| match ms(end) {
                Ok(end) => end / 1000,
                Err(_) => 0,
            })
            .unwrap_or(0);
    eprintln!(
        "Listing events between {} GMT and {} GMT and now is {} GMT",
        NaiveDateTime::from_timestamp(start_time, 0),
        NaiveDateTime::from_timestamp(end_time, 0),
        NaiveDateTime::from_timestamp(now, 0)
    );

    let client = CloudWatchLogsClient::new(Region::default());
    let mut log_events = Vec::<OutputLogEvent>::new();

    for stream in get_streams(log_group_name).await {
        match stream {
            LogStream {
                first_event_timestamp: _,
                log_stream_name: Some(log_stream_name),
                arn: _,
                creation_time: _,
                last_event_timestamp: _,
                last_ingestion_time: _,
                upload_sequence_token: _,
            } => {
                match get_log_events(
                    &client,
                    &log_group_name,
                    &log_stream_name,
                    start_time,
                    end_time,
                    None,
                )
                .await
                {
                    Ok(GetLogEventsResponse {
                        events,
                        next_backward_token: _,
                        next_forward_token,
                    }) => {
                        if let Some(mut events) = events {
                            log_events.append(&mut events);
                        }
                        let mut token = next_forward_token;
                        let mut is_retry = false;
                        while token.is_some() {
                            match get_log_events(
                                &client,
                                &log_group_name,
                                &log_stream_name,
                                start_time,
                                end_time,
                                token.clone(),
                            )
                            .await
                            {
                                Ok(GetLogEventsResponse {
                                    events,
                                    next_backward_token: _,
                                    next_forward_token,
                                }) => {
                                    if let Some(mut events) = events {
                                        log_events.append(&mut events);
                                    }
                                    token = match (token, next_forward_token.clone()) {
                                        (Some(current), Some(next)) if (current != next) => {
                                            next_forward_token
                                        }
                                        _ if (is_retry) => {
                                            is_retry = false;
                                            next_forward_token
                                        }
                                        _ => None,
                                    }
                                }
                                Err(RusotoError::Unknown(BufferedHttpResponse {
                                    status: StatusCode::BAD_REQUEST,
                                    body: _,
                                    headers: _,
                                })) => {
                                    is_retry = true;
                                    throttle()
                                }
                                Err(error) => eprintln!("Error in getting events: {:?}", error),
                            }
                        }
                    }
                    Err(error) => eprintln!("Error in getting events: {:?}", error),
                }
            }
            _ => {}
        }
    }
    for e in log_events {
        match e {
            OutputLogEvent {
                ingestion_time: Some(ingestion_time),
                message: Some(message),
                timestamp: Some(timestamp),
            } => println!("{} {} {}", ingestion_time, message, timestamp),
            _ => {}
        }
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

async fn get_log_events(
    client: &CloudWatchLogsClient,
    log_group_name: &String,
    log_stream_name: &String,
    start_time: i64,
    end_time: i64,
    next_token: Option<String>,
) -> Result<GetLogEventsResponse, RusotoError<GetLogEventsError>> {
    client
        .get_log_events(GetLogEventsRequest {
            end_time: Some(end_time),
            limit: Some(10_000),
            log_group_name: log_group_name.clone(),
            log_stream_name: log_stream_name.clone(),
            next_token: next_token,
            start_from_head: Some(true),
            start_time: Some(start_time),
        })
        .await
}

fn throttle() {
    thread::sleep(time::Duration::from_millis(100));
}
