use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthUserAddRequest, error::Result, Client};
use xlineapi::AuthUserAddResponse;

use crate::utils::printer::Printer;

/// Definition of `add` command
pub(super) fn command() -> Command {
    Command::new("add")
        .about("Add a new user")
        .arg(arg!(<name> "The name of the user"))
        .arg(
            arg!([password] "Password of the user")
                .required_if_eq("no_password", "false")
                .required_unless_present("no_password"),
        )
        .arg(arg!(--no_password "Create without password"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserAddRequest {
    let name = matches.get_one::<String>("name").expect("required");
    let no_password = matches.get_flag("no_password");
    if no_password {
        AuthUserAddRequest::new(name).with_no_pwd()
    } else {
        let password = matches.get_one::<String>("password").expect("required");
        AuthUserAddRequest::new(name).with_pwd(password)
    }
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.user_add(req).await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of user add response
fn print_resp(resp: &AuthUserAddResponse) {
    Printer::header(resp.header.as_ref());
    println!("User added");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthUserAddRequest);

    #[test]
    fn valid() {
        let testcases = vec![
            TestCase::new(
                vec!["add", "JaneSmith", "password123"],
                Some(AuthUserAddRequest::new("JaneSmith").with_pwd("password123")),
            ),
            TestCase::new(
                vec!["add", "--no_password", "BobJohnson"],
                Some(AuthUserAddRequest::new("BobJohnson").with_no_pwd()),
            ),
        ];

        for case in testcases {
            case.run_test();
        }
    }

    #[test]
    fn invalid() {
        let testcases = vec![TestCase::new(vec!["add", "JaneSmith"], None)];

        for case in testcases {
            case.run_test();
        }
    }
}
