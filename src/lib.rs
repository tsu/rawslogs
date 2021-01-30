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
use std::io::{stderr, stdout, Write};
use std::{thread, time};

const LIMIT: i64 = 50;
const ONE_HOUR_IN_SECONDS: i64 = 60 * 60;

pub struct ListGroupsParams {
    client: CloudWatchLogsClient,
    out: Box<dyn Write>,
    err: Box<dyn Write>,
}

pub struct ListGroupsParamsBuilder {
    client: Option<CloudWatchLogsClient>,
    out: Option<Box<dyn Write>>,
    err: Option<Box<dyn Write>>,
}

impl ListGroupsParamsBuilder {
    pub fn new() -> ListGroupsParamsBuilder {
        ListGroupsParamsBuilder {
            client: None,
            out: None,
            err: None,
        }
    }

    pub fn with_client(mut self, client: CloudWatchLogsClient) -> ListGroupsParamsBuilder {
        self.client = Some(client);
        self
    }

    pub fn with_out(mut self, out: Box<dyn Write>) -> ListGroupsParamsBuilder {
        self.out = Some(out);
        self
    }

    pub fn with_err(mut self, err: Box<dyn Write>) -> ListGroupsParamsBuilder {
        self.err = Some(err);
        self
    }

    pub fn build(self) -> ListGroupsParams {
        return ListGroupsParams {
            client: self
                .client
                .unwrap_or(CloudWatchLogsClient::new(Region::default())),
            out: self.out.unwrap_or(Box::new(stdout())),
            err: self.err.unwrap_or(Box::new(stderr())),
        };
    }
}

pub async fn list_groups(params: ListGroupsParams) {
    let client = params.client;
    let mut out = params.out;
    let mut err = params.err;
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
                    Err(error) => {
                        let _ = writeln!(err, "Error: {:?}", error);
                    }
                }
            }
        }
        Err(error) => {
            let _ = writeln!(err, "Error: {:?}", error);
        }
    }

    for g in groups {
        let _ = writeln!(out, "{}", g.log_group_name.expect("mf"));
    }
}

pub struct ListStreamsParams {
    log_group_name: String,
    client: CloudWatchLogsClient,
    out: Box<dyn Write>,
    err: Box<dyn Write>,
}

pub struct ListStreamsParamsBuilder {
    log_group_name: String,
    client: Option<CloudWatchLogsClient>,
    out: Option<Box<dyn Write>>,
    err: Option<Box<dyn Write>>,
}

impl ListStreamsParamsBuilder {
    pub fn new(log_group_name: String) -> ListStreamsParamsBuilder {
        ListStreamsParamsBuilder {
            log_group_name,
            client: None,
            out: None,
            err: None,
        }
    }

    pub fn with_client(mut self, client: CloudWatchLogsClient) -> ListStreamsParamsBuilder {
        self.client = Some(client);
        self
    }

    pub fn with_out(mut self, out: Box<dyn Write>) -> ListStreamsParamsBuilder {
        self.out = Some(out);
        self
    }

    pub fn with_err(mut self, err: Box<dyn Write>) -> ListStreamsParamsBuilder {
        self.err = Some(err);
        self
    }

    pub fn build(self) -> ListStreamsParams {
        return ListStreamsParams {
            log_group_name: self.log_group_name,
            client: self
                .client
                .unwrap_or(CloudWatchLogsClient::new(Region::default())),
            out: self.out.unwrap_or(Box::new(stdout())),
            err: self.err.unwrap_or(Box::new(stderr())),
        };
    }
}

pub async fn list_streams(params: ListStreamsParams) {
    let client = params.client;
    let mut out = params.out;
    let mut err = params.err;
    let log_group_name = params.log_group_name;

    for g in get_streams(&client, &mut err, &log_group_name).await {
        let _ = writeln!(out, "{}", g.log_stream_name.expect("mf"));
    }
}

async fn get_streams(
    client: &CloudWatchLogsClient,
    err: &mut Box<dyn Write>,
    log_group_name: &String,
) -> Vec<LogStream> {
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
                    Err(error) => {
                        let _ = writeln!(err, "Error: {:?}", error);
                    }
                }
            }
        }
        Err(error) => {
            let _ = writeln!(err, "Error: {:?}", error);
        }
    }

    return streams;
}

pub struct ListEventsParams {
    log_group_name: String,
    start: Option<String>,
    end: Option<String>,
    client: CloudWatchLogsClient,
    out: Box<dyn Write>,
    err: Box<dyn Write>,
}

pub struct ListEventsParamsBuilder {
    log_group_name: String,
    start: Option<String>,
    end: Option<String>,
    client: Option<CloudWatchLogsClient>,
    out: Option<Box<dyn Write>>,
    err: Option<Box<dyn Write>>,
}

impl ListEventsParamsBuilder {
    pub fn new(
        log_group_name: String,
        start: Option<String>,
        end: Option<String>,
    ) -> ListEventsParamsBuilder {
        ListEventsParamsBuilder {
            log_group_name,
            start,
            end,
            client: None,
            out: None,
            err: None,
        }
    }

    pub fn with_client(mut self, client: CloudWatchLogsClient) -> ListEventsParamsBuilder {
        self.client = Some(client);
        self
    }

    pub fn with_out(mut self, out: Box<dyn Write>) -> ListEventsParamsBuilder {
        self.out = Some(out);
        self
    }

    pub fn with_err(mut self, err: Box<dyn Write>) -> ListEventsParamsBuilder {
        self.err = Some(err);
        self
    }

    pub fn build(self) -> ListEventsParams {
        return ListEventsParams {
            log_group_name: self.log_group_name,
            start: self.start,
            end: self.end,
            client: self
                .client
                .unwrap_or(CloudWatchLogsClient::new(Region::default())),
            out: self.out.unwrap_or(Box::new(stdout())),
            err: self.err.unwrap_or(Box::new(stderr())),
        };
    }
}

pub async fn list_events(params: ListEventsParams) {
    let client = params.client;
    let log_group_name = params.log_group_name;
    let mut out = params.out;
    let mut err = params.err;
    let start = params.start;
    let end = params.end;
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
    let _ = writeln!(
        err,
        "Listing events between {} GMT and {} GMT and now is {} GMT",
        NaiveDateTime::from_timestamp(start_time, 0),
        NaiveDateTime::from_timestamp(end_time, 0),
        NaiveDateTime::from_timestamp(now, 0)
    );

    let mut log_events = Vec::<OutputLogEvent>::new();

    for stream in get_streams(&client, &mut err, &log_group_name).await {
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
                                Err(error) => {
                                    let _ = writeln!(err, "Error in getting events: {:?}", error);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let _ = writeln!(err, "Error in getting events: {:?}", error);
                    }
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
            } => {
                let _ = writeln!(out, "{} {} {}", ingestion_time, message, timestamp);
            }
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
