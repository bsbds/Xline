use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};
use xlineapi::AuthUserListResponse;

use crate::utils::printer::Printer;

/// Definition of `list` command
pub(super) fn command() -> Command {
    Command::new("list").about("List all users")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.user_list().await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of user list response
fn print_resp(resp: &AuthUserListResponse) {
    Printer::header(resp.header.as_ref());
    println!("Users:");
    for user in &resp.users {
        println!("{user}");
    }
}
