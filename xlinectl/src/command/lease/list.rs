use clap::{ArgMatches, Command};
use xline_client::{error::Result, Client};
use xlineapi::LeaseLeasesResponse;

use crate::utils::printer::Printer;

/// Definition of `List` command
pub(super) fn command() -> Command {
    Command::new("list").about("List all active leases")
}

/// Execute the command
pub(super) async fn execute(client: &mut Client, _matches: &ArgMatches) -> Result<()> {
    let resp = client.leases().await?;

    print_resp(&resp);

    Ok(())
}

/// Printer of list response
fn print_resp(resp: &LeaseLeasesResponse) {
    Printer::header(resp.header.as_ref());
    for lease in &resp.leases {
        println!("lease: {}", lease.id);
    }
}
