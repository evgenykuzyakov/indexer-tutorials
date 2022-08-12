# events-indexer

To start a local network just set this field under near_indexer::InitConfigArgs
```
download_config_url: Some("https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/testnet/config.json".to_string())
```

Build
```
cargo build --release
```

Init - downloads the config, also populates ~/.near with genesis and validator key
```
cargo run --release init
```

Run
```
cargo run --release run
```

Deploy a contract with logs to the test chain (read the chainId from genesis.json) E.g.:
```
near deploy --accountId counter.test.near --wasmFile counter_contract.wasm --nodeUrl http://0.0.0.0:3030 --networkId test-chain-t7idz
```

Call a method that emits an event (log) and the indexer will pick it up.