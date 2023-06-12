use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{clients::lease::LeaseTimeToLiveRequest, error::Result, Client};
use xlineapi::LeaseTimeToLiveResponse;

use crate::utils::printer::Printer;

/// Definition of `timetolive` command
pub(super) fn command() -> Command {
    Command::new("timetolive")
        .about("Get lease ttl information")
        .arg(arg!(<leaseId> "Lease id to get").value_parser(value_parser!(i64)))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> LeaseTimeToLiveRequest {
    let lease_id = matches.get_one::<i64>("leaseId").expect("required");
    LeaseTimeToLiveRequest::new(*lease_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.lease_time_to_live(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of timetolive response
fn print_resp(resp: &LeaseTimeToLiveResponse) {
    Printer::header(resp.header.as_ref());
    println!(
        "lease id: {}, ttl: {}, granted_ttl: {}",
        resp.id, resp.ttl, resp.granted_ttl
    );

    for key in &resp.keys {
        Printer::key(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(LeaseTimeToLiveRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["timetolive", "123"],
            Some(LeaseTimeToLiveRequest::new(123)),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
