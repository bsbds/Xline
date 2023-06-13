use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};
use xlineapi::AuthDisableResponse;

use crate::utils::printer::Printer;

/// Definition of `disable` command
pub(super) fn command() -> Command {
    Command::new("disable").about("disable authentication")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.auth_disable().await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of grant response
fn print_resp(resp: &AuthDisableResponse) {
    Printer::header(resp.header.as_ref());
    println!("Authentication disabled");
}
