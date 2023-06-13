use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthUserDeleteRequest, error::Result, Client};
use xlineapi::AuthUserDeleteResponse;

use crate::utils::printer::Printer;

/// Definition of `delete` command
pub(super) fn command() -> Command {
    Command::new("delete")
        .about("Delete a user")
        .arg(arg!(<name> "The name of the user"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserDeleteRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthUserDeleteRequest::new(name)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.user_delete(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of user delete response
fn print_resp(resp: &AuthUserDeleteResponse) {
    Printer::header(resp.header.as_ref());
    println!("User deleted");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthUserDeleteRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["delete", "JohnDoe"],
            Some(AuthUserDeleteRequest::new("JohnDoe")),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
