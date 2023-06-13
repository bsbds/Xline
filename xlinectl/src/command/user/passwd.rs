use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthUserChangePasswordRequest, error::Result, Client};
use xlineapi::AuthUserChangePasswordResponse;

use crate::utils::printer::Printer;

/// Definition of `passwd` command
// TODO: interactive mode
pub(super) fn command() -> Command {
    Command::new("passwd")
        .about("Change the password of a user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(<password> "Password to change"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserChangePasswordRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let password = matches.get_one::<String>("password").expect("required");
    AuthUserChangePasswordRequest::new(name, password)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.user_change_password(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of change password response
fn print_resp(resp: &AuthUserChangePasswordResponse) {
    Printer::header(resp.header.as_ref());
    println!("Password changed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthUserChangePasswordRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["passwd", "JohnDoe", "new_password"],
            Some(AuthUserChangePasswordRequest::new(
                "JohnDoe",
                "new_password",
            )),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
