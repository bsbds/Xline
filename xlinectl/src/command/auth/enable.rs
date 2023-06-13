use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};
use xlineapi::AuthEnableResponse;

use crate::utils::printer::Printer;

/// Definition of `enable` command
pub(super) fn command() -> Command {
    Command::new("enable").about("Enable authentication")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.auth_enable().await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of grant response
fn print_resp(resp: &AuthEnableResponse) {
    Printer::header(resp.header.as_ref());
    println!("Authentication enabled");
}
