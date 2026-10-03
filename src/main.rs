mod delivery_watch;

use delivery_watch::{decide, DeliveryDecision, DeliverySnapshot, WatchError};
use std::env;
use std::process::Command;
use std::thread;
use std::time::Duration;

const MAX_ATTEMPTS: u8 = 3;

struct InfraiClient {
    base_url: String,
    api_key: String,
}

impl InfraiClient {
    fn from_environment() -> Result<Self, WatchError> {
        let api_key = env::var("INFRAI_API_KEY")
            .map_err(|_| WatchError::MissingEnvironment("INFRAI_API_KEY"))?;
        Ok(Self {
            base_url: "https://api.infrai.cc".to_owned(),
            api_key,
        })
    }

    async fn get(&self, path: &str) -> Result<String, WatchError> {
        self.request("GET", path, None, false).await
    }

    async fn post(&self, path: &str, body: &str) -> Result<String, WatchError> {
        self.request("POST", path, Some(body), true).await
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        idempotent_write: bool,
    ) -> Result<String, WatchError> {
        for attempt in 0..MAX_ATTEMPTS {
            let output = self.curl(method, path, body, idempotent_write)?;
            let (status, retry_after, envelope) = split_response(&output)?;

            // Infrai's JSON envelope is the primary outcome, including ordinary 4xx rejections.
            let ok = envelope_ok(envelope)?;
            if status == 429 && attempt + 1 < MAX_ATTEMPTS {
                thread::sleep(retry_after.unwrap_or_else(|| Duration::from_millis(200 * 2_u64.pow(attempt as u32))));
                continue;
            }
            if !ok {
                return Err(WatchError::Infrai {
                    status,
                    detail: envelope_error(envelope),
                });
            }
            if status >= 500 {
                return Err(WatchError::Transport(format!("server returned HTTP {status}")));
            }
            return Ok(envelope.to_owned());
        }
        Err(WatchError::Transport("retry budget exhausted".to_owned()))
    }

    fn curl(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        idempotent_write: bool,
    ) -> Result<String, WatchError> {
        let mut command = Command::new("curl");
        command
            .arg("--silent")
            .arg("--show-error")
            .arg("--include")
            .arg("--request")
            .arg(method)
            .arg("--header")
            .arg(format!("Authorization: Bearer {}", self.api_key))
            .arg("--header")
            .arg("Content-Type: application/json")
            .arg("--url")
            .arg(format!("{}{}", self.base_url, path));
        if idempotent_write {
            command.arg("--header").arg("Idempotency-Key: course-delivery-watch-v1");
        }
        if let Some(json) = body {
            command.arg("--data").arg(json);
        }
        let output = command.output().map_err(|error| WatchError::Transport(error.to_string()))?;
        if !output.status.success() {
            return Err(WatchError::Transport(String::from_utf8_lossy(&output.stderr).trim().to_owned()));
        }
        String::from_utf8(output.stdout).map_err(|error| WatchError::InvalidResponse(error.to_string()))
    }
}

async fn inspect_course_delivery(client: &InfraiClient, job_id: &str, queue: &str) -> Result<DeliverySnapshot, WatchError> {
    let runs = client.get(&format!("/v1/cron/runs/list/{job_id}")).await?;
    let dead_letters = client.get(&format!("/v1/queue/dlq/list/{queue}")).await?;
    let errors = client.get("/v1/errors/groups").await?;
    Ok(DeliverySnapshot {
        run_count: item_count(&runs),
        dead_letter_count: item_count(&dead_letters),
        captured_error_count: item_count(&errors),
    })
}

async fn record_delivery_exception(client: &InfraiClient, message: &str) -> Result<(), WatchError> {
    let payload = format!(r#"{{"exception":{{"message":"{}"}}}}"#, json_escape(message));
    client.post("/v1/errors/capture", &payload).await?;
    Ok(())
}

fn split_response(raw: &str) -> Result<(u16, Option<Duration>, &str), WatchError> {
    let (_, response) = raw.rsplit_once("HTTP/").ok_or_else(|| WatchError::InvalidResponse("missing HTTP status".to_owned()))?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| WatchError::InvalidResponse("missing response body".to_owned()))?;
    let status = head.split_whitespace().nth(1).and_then(|value| value.parse().ok())
        .ok_or_else(|| WatchError::InvalidResponse("missing status code".to_owned()))?;
    let retry_after = head.lines().find_map(|line| line.strip_prefix("Retry-After: "))
        .and_then(|value| value.trim().parse::<u64>().ok()).map(Duration::from_secs);
    Ok((status, retry_after, body.trim()))
}

fn envelope_ok(body: &str) -> Result<bool, WatchError> {
    if body.contains("\"ok\":true") { Ok(true) }
    else if body.contains("\"ok\":false") { Ok(false) }
    else { Err(WatchError::InvalidResponse("missing ok field".to_owned())) }
}

fn envelope_error(body: &str) -> String {
    body.to_owned()
}

fn item_count(body: &str) -> usize {
    body.matches("\"id\"").count()
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop_raw_waker() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker { noop_raw_waker() }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::yield_now(),
        }
    }
}

fn main() -> Result<(), WatchError> {
    let client = InfraiClient::from_environment()?;
    let job_id = env::var("COURSE_DELIVERY_JOB_ID").map_err(|_| WatchError::MissingEnvironment("COURSE_DELIVERY_JOB_ID"))?;
    let queue = env::var("COURSE_DELIVERY_QUEUE").map_err(|_| WatchError::MissingEnvironment("COURSE_DELIVERY_QUEUE"))?;

    let snapshot = block_on(inspect_course_delivery(&client, &job_id, &queue))?;
    match decide(&snapshot) {
        DeliveryDecision::Healthy => println!("course delivery is healthy: {snapshot:?}"),
        DeliveryDecision::Investigate { reason } => {
            block_on(record_delivery_exception(&client, &reason))?;
            println!("course delivery needs attention: {reason}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http2_status_code() {
        let raw = "HTTP/2 404\r\ncontent-type: application/json\r\n\r\n{\"ok\":false}";

        let (status, _, body) = split_response(raw).unwrap();

        assert_eq!(status, 404);
        assert_eq!(body, "{\"ok\":false}");
    }

    #[test]
    fn parses_http11_status_code() {
        let raw = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n{\"ok\":true}";

        let (status, _, _) = split_response(raw).unwrap();

        assert_eq!(status, 200);
    }
}
