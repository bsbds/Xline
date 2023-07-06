# xline-client

Official Xline API client for Rust backed by the [CURP](https://github.com/xline-kv/Xline/tree/master/curp) protocol

## Features

`xline-client` runs the CURP protocol on the client side for maximized performance.

## Supported APIs

- KV
  - [x] Put
  - [x] Range
  - [x] Delete
  - [x] Transaction
  - [ ] Compact
- Lease
  - [ ] Grant
  - [ ] Revoke
  - [ ] KeepAlive
  - [ ] TimeToLive
- Watch
  - [x] WatchCreate
  - [x] WatchCancel
- Auth
  - [ ] Authenticate
  - [ ] RoleAdd
  - [ ] RoleGrantPermission
  - [ ] UserAdd
  - [ ] UserGrantRole
  - [ ] AuthEnable
  - [ ] AuthDisable
- Cluster
  - [ ] MemberAdd
  - [ ] MemberRemove
  - [ ] MemberUpdate
  - [ ] MemberList
- Maintenance
  - [ ] Alarm
  - [ ] Status
  - [ ] Defragment
  - [ ] Hash
  - [x] Snapshot
  - [ ] MoveLeader

Note that certain APIs that have not been implemented in Xline will also not be implemented in `xline-client`.

## Get started

Add `xline-client` to your `Cargo.toml`:

```toml
[dependencies]
xline-client = { git = "https://github.com/xline-kv/Xline.git", package = "xline-client" }
```
To create a xline client:
```rust
use xline_client::{
    Client,
    error::Error,
    types::kv::{PutRequest, RangeRequest},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // the name and address of all curp members
    let curp_members = [
        ("server0", "10.0.0.1:2379"),
        ("server1", "10.0.0.2:2379"),
        ("server2", "10.0.0.3:2379"),
    ]
    .map(|(s, a)| (s.to_owned(), a.to_owned()));

    let client = Client::connect(curp_members.into(), ClientOptions::default()).await?;

    client.put(PutRequest::new("key", "value")).await?;

    let resp = client.range(RangeRequest::new("key")).await?;

    if let Some(kv) = resp.kvs.first() {
        println!(
            "got key: {}, value: {}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }

    Ok(())
}
```

## Xline Compatibility

We aim to maintain compatibility with each corresponding Xline version, and update this library with each new Xline release.

Current library version has been tested to work with Xline v0.4.1.

## Documentation

Checkout the [API document](https://docs.rs) (currently unavailable) on docs.rs
