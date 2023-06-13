use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthUserGrantRoleRequest, error::Result, Client};
use xlineapi::AuthUserGrantRoleResponse;

use crate::utils::printer::Printer;

/// Definition of `grant_role` command
pub(super) fn command() -> Command {
    Command::new("grant_role")
        .about("Grant role to a user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(<role> "The name of the role"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserGrantRoleRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let role = matches.get_one::<String>("role").expect("required");
    AuthUserGrantRoleRequest::new(name, role)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.user_grant_role(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of user grant role response
fn print_resp(resp: &AuthUserGrantRoleResponse) {
    Printer::header(resp.header.as_ref());
    println!("Role granted");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthUserGrantRoleRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["grant_role", "JohnDoe", "Admin"],
            Some(AuthUserGrantRoleRequest::new("JohnDoe", "Admin")),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
