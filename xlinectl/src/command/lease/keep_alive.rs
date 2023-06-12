use std::time::Duration;

use clap::{arg, value_parser, ArgMatches, Command};
use tonic::Streaming;
use xline_client::{
    clients::lease::{LeaseKeepAliveRequest, LeaseKeeper},
    error::{ClientError, Result},
    Client,
};
use xlineapi::LeaseKeepAliveResponse;

use crate::utils::printer::Printer;

/// Definition of `keep_alive` command
pub(super) fn command() -> Command {
    Command::new("keep_alive")
        .about("lease keep alive periodically")
        .arg(arg!(<leaseId> "Lease Id to keep alive").value_parser(value_parser!(i64)))
        .arg(arg!(--once "keep alive once"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> LeaseKeepAliveRequest {
    let lease_id = matches.get_one::<i64>("leaseId").expect("required");
    LeaseKeepAliveRequest::new(*lease_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let once = matches.get_flag("once");

    let req = build_request(matches);

    let (mut keeper, mut stream) = client.lease_keep_alive(req).await?;

    if once {
        keeper.keep_alive()?;
        if let Some(resp) = stream.message().await? {
            print_resp(&resp);
        }
    } else {
        let _handle = tokio::spawn(async move {
            keep_alive_loop(keeper, stream).await?;
            Ok::<(), ClientError>(())
        });
    }

    Ok(())
}

/// keep alive forever unless encounter error
async fn keep_alive_loop(
    mut keeper: LeaseKeeper,
    mut stream: Streaming<LeaseKeepAliveResponse>,
) -> Result<()> {
    loop {
        keeper.keep_alive()?;
        if let Some(resp) = stream.message().await? {
            print_resp(&resp);
            if resp.ttl < 0 {
                return Err(ClientError::InvalidArgs(String::from(
                    "lease keepalive response has negative ttl",
                )));
            }
            tokio::time::sleep(Duration::from_secs(resp.ttl.unsigned_abs() / 3)).await;
        }
    }
}

/// Printer of keep alive response
fn print_resp(resp: &LeaseKeepAliveResponse) {
    Printer::header(resp.header.as_ref());
    println!("id: {} keepalived with TTL: {}", resp.id, resp.ttl);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(LeaseKeepAliveRequest);

    #[test]
    fn valid() {
        let testcases = vec![
            TestCase::new(
                vec!["keep_alive", "123"],
                Some(LeaseKeepAliveRequest::new(123)),
            ),
            TestCase::new(
                vec!["keep_alive", "456", "--once"],
                Some(LeaseKeepAliveRequest::new(456)),
            ),
        ];

        for case in testcases {
            case.run_test();
        }
    }
}
