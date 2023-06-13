use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthUserRevokeRoleRequest, error::Result, Client};
use xlineapi::AuthUserRevokeRoleResponse;

use crate::utils::printer::Printer;

/// Definition of `revoke_role` command
pub(super) fn command() -> Command {
    Command::new("revoke_role")
        .about("Revoke role from a user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(<role> "The name of the role"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserRevokeRoleRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let role = matches.get_one::<String>("role").expect("required");
    AuthUserRevokeRoleRequest::new(name, role)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.user_revoke_role(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of user revoke role response
fn print_resp(resp: &AuthUserRevokeRoleResponse) {
    Printer::header(resp.header.as_ref());
    println!("Role revoked");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthUserRevokeRoleRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["revoke_role", "JohnDoe", "Admin"],
            Some(AuthUserRevokeRoleRequest::new("JohnDoe", "Admin")),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
