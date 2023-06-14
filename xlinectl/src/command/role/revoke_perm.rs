use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthRoleRevokePermissionRequest, error::Result, Client};
use xlineapi::AuthRoleRevokePermissionResponse;

use crate::utils::printer::Printer;

/// Definition of `revoke_perm` command
pub(super) fn command() -> Command {
    Command::new("revoke_perm")
        .about("Revoke permission from a role")
        .arg(arg!(<name> "The name of the role"))
        .arg(arg!(<key> "The Key"))
        .arg(arg!([range_end] "Range end of the key"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthRoleRevokePermissionRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let key = matches.get_one::<String>("key").expect("required");
    let range_end = matches.get_one::<String>("range_end");

    let mut request = AuthRoleRevokePermissionRequest::new(name, key.as_bytes());

    if let Some(range_end) = range_end {
        request = request.with_range_end(range_end.as_bytes());
    };

    request
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.role_revoke_permission(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of role revoke permission response
fn print_resp(resp: &AuthRoleRevokePermissionResponse) {
    Printer::header(resp.header.as_ref());
    println!("Permission revokeed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthRoleRevokePermissionRequest);

    #[test]
    fn valid() {
        let testcases = vec![
            TestCase::new(
                vec!["revoke_perm", "Admin", "key1", "key2"],
                Some(AuthRoleRevokePermissionRequest::new("Admin", "key1").with_range_end("key2")),
            ),
            TestCase::new(
                vec!["revoke_perm", "Admin", "key3"],
                Some(AuthRoleRevokePermissionRequest::new("Admin", "key3")),
            ),
        ];

        for case in testcases {
            case.run_test();
        }
    }
}
