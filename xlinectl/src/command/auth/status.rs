use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};
use xlineapi::AuthStatusResponse;

use crate::utils::printer::Printer;

/// Definition of `status` command
pub(super) fn command() -> Command {
    Command::new("status").about("Status of authentication")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.auth_status().await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of auth status response
fn print_resp(resp: &AuthStatusResponse) {
    Printer::header(resp.header.as_ref());
    println!(
        "enabled: {}, revision: {}",
        resp.enabled, resp.auth_revision
    );
}
