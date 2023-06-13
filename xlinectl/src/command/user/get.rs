use clap::{arg, ArgMatches, Command};
use xline_client::{
    clients::auth::{AuthRoleGetRequest, AuthUserGetRequest},
    error::Result,
    Client,
};
use xlineapi::{AuthRoleGetResponse, AuthUserGetResponse};

use crate::utils::printer::Printer;

/// Definition of `get` command
pub(super) fn command() -> Command {
    Command::new("get")
        .about("Get a new user")
        .arg(arg!(<name> "The name of the user"))
        .arg(arg!(--detail "Show permissions of roles granted to the user"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthUserGetRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthUserGetRequest::new(name.as_str())
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let name = matches.get_one::<String>("name").expect("required");
    let detail = matches.get_flag("detail");

    let req = build_request(matches);

    let resp = client.user_get(req).await?;

    if detail {
        print_resp_user_get(name, &resp);
        for role in resp.roles {
            let resp_role_get = client.role_get(AuthRoleGetRequest::new(&role)).await?;
            print_resp_role_get(&role, &resp_role_get);
        }
    } else {
        print_resp_user_get(name, &resp);
    }

    Ok(())
}

/// Printer of user get response
fn print_resp_user_get(name: &str, resp: &AuthUserGetResponse) {
    Printer::header(resp.header.as_ref());
    println!("User: {name}");
    println!("Roles: ");
    for role in &resp.roles {
        print!("{role}");
    }
}

/// Printer of role get response
fn print_resp_role_get(role: &str, resp: &AuthRoleGetResponse) {
    Printer::header(resp.header.as_ref());
    println!("Role: {role}");
    for perm in &resp.perm {
        println!("perm type: {}", perm.perm_type);
        Printer::key(&perm.key);
        Printer::range_end(&perm.range_end);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(AuthUserGetRequest);

    #[test]
    fn valid() {
        let testcases = vec![
            TestCase::new(
                vec!["get", "JohnDoe"],
                Some(AuthUserGetRequest::new("JohnDoe")),
            ),
            TestCase::new(
                vec!["get", "--detail", "JaneSmith"],
                Some(AuthUserGetRequest::new("JaneSmith")),
            ),
        ];

        for case in testcases {
            case.run_test();
        }
    }
}
