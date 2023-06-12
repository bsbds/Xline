use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{clients::lease::LeaseRevokeRequest, error::Result, Client};
use xlineapi::LeaseRevokeResponse;

use crate::utils::printer::Printer;

/// Definition of `revoke` command
pub(super) fn command() -> Command {
    Command::new("revoke")
        .about("Revoke a lease")
        .arg(arg!(<leaseId> "Lease Id to revoke").value_parser(value_parser!(i64)))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> LeaseRevokeRequest {
    let lease_id = matches.get_one::<i64>("leaseId").expect("required");
    LeaseRevokeRequest::new(*lease_id)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.lease_revoke(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of revoke response
fn print_resp(resp: &LeaseRevokeResponse) {
    Printer::header(resp.header.as_ref());
    println!("Revoked");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(LeaseRevokeRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["revoke", "123"],
            Some(LeaseRevokeRequest::new(123)),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
