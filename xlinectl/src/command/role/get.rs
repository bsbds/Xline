use clap::{arg, ArgMatches, Command};
use xline_client::{clients::auth::AuthRoleGetRequest, error::Result, Client};
use xlineapi::AuthRoleGetResponse;

use crate::utils::printer::Printer;

/// Definition of `get` command
pub(super) fn command() -> Command {
    Command::new("get")
        .about("Get a new role")
        .arg(arg!(<name> "The name of the role"))
}

/// Build request from matches
pub(super) fn build_request(matches: &ArgMatches) -> AuthRoleGetRequest {
    let name = matches.get_one::<String>("name").expect("required");
    AuthRoleGetRequest::new(name)
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let name = matches.get_one::<String>("name").expect("required");
    let req = build_request(matches);

    let resp = client.role_get(req).await?;

    print_resp(name, &resp);

    Ok(())
}

/// Printer of role get response
pub(crate) fn print_resp(role: &str, resp: &AuthRoleGetResponse) {
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

    testcase_struct!(AuthRoleGetRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["get", "Admin"],
            Some(AuthRoleGetRequest::new("Admin")),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
