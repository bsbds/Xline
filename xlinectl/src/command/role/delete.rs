use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthRoleDeleteRequest, error::Result, Client};
use xlineapi::AuthRoleDeleteResponse;

use crate::utils::printer::Printer;

/// Definition of `delete` command
pub(super) fn command() -> Command {
    Command::new("delete")
        .about("delete a role")
        .arg(arg!(<name> "The name of the role"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthRoleDeleteRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthRoleDeleteRequest::new(name)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.role_delete(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of role delete response
fn print_resp(resp: &AuthRoleDeleteResponse) {
    Printer::header(resp.header.as_ref());
    println!("User deleteed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthRoleDeleteRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["delete", "Admin"],
            Some(AuthRoleDeleteRequest::new("Admin")),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
