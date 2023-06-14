use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};
use xlineapi::AuthRoleListResponse;

use crate::utils::printer::Printer;

/// Definition of `list` command
pub(super) fn command() -> Command {
    Command::new("list").about("List all roles")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.role_list().await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of role list response
fn print_resp(resp: &AuthRoleListResponse) {
    Printer::header(resp.header.as_ref());
    for role in &resp.roles {
        println!("{role}");
    }
}
