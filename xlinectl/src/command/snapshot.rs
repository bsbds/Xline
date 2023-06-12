use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use clap::{arg, ArgMatches, Command};
use xline_client::{error::Result, Client};

/// Definition of `snapshot` command
pub(crate) fn command() -> Command {
    Command::new("snapshot")
        .about("get snapshots of xline nodes")
        .subcommand(
            Command::new("save")
                .about("save snapshot")
                .arg(arg!(<filename> "save snapshot to the give filename")),
        )
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    if let Some(("save", sub_matches)) = matches.subcommand() {
        let filename = sub_matches.get_one::<String>("filename").expect("required");
        let path = PathBuf::from(filename);
        let mut resp = client.maintenance_client().snapshot().await?;

        if path.exists() || path.is_dir() {
            eprintln!("file exist: {filename}");
            return Ok(());
        }

        let mut file = File::create(path)?;

        let mut all = Vec::new();
        while let Some(data) = resp.message().await? {
            all.extend_from_slice(&data.blob);
        }

        file.write_all(&all)?;

        println!("snapshot saved to: {filename}");
    }

    Ok(())
}
