use clap::{arg, ArgMatches, Command};
use tokio::signal;
use xline_client::{
    clients::lock::{LockRequest, UnlockRequest},
    error::Result,
    Client,
};
use xlineapi::LockResponse;

use crate::utils::printer::Printer;

/// Definition of `lock` command
pub(crate) fn command() -> Command {
    Command::new("lock")
        .about("Acquire a lock, which will return a unique key that exists so long as the lock is held")
        .arg(arg!(<lockname> "name of the lock"))
}

/// Build request from matches
pub(crate) fn build_request(matches: &ArgMatches) -> LockRequest {
    let name = matches.get_one::<String>("lockname").expect("required");
    LockRequest::new().with_name(name.as_bytes())
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let req = build_request(matches);

    let resp = client.lock(req).await?;

    print_resp(&resp);

    signal::ctrl_c().await.expect("failed to listen for event");

    println!("releasing the lock");

    let unlock_req = UnlockRequest::new().with_key(resp.key);
    let _unlock_resp = client.unlock(unlock_req).await?;

    Ok(())
}

/// Printer of lock response
fn print_resp(resp: &LockResponse) {
    Printer::header(resp.header.as_ref());
    Printer::key(&resp.key);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testcase_struct;

    testcase_struct!(LockRequest);

    #[test]
    fn valid() {
        let testcases = vec![TestCase::new(
            vec!["lock", "my_lock"],
            Some(LockRequest::new().with_name("my_lock".as_bytes())),
        )];

        for case in testcases {
            case.run_test();
        }
    }
}
