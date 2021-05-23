raft
====


This is my second attempt implementing the [Raft](https://raft.github.io/raft.pdf) consensus algorithm.

It's not production code and does not support persistence of the key/value store.

The following were not implemented:

- Persistency(all states are volatiles).
- Cluster membership changes
- Log compaction

TODO
----

- Add Log trait for that defines the Raft log behavior
- Leader discovery and confirmation
- Append only *write* operation entry
- Persist log and non volatiles states.
- ...

Run
---

To run this we need a configuration see [example](config.example.yaml).

We will then start the following processes. The numbers of processes depends on the configuration. 

```
./target/debug/kvserver --config config.yaml --node-id 0 --port 21000
./target/debug/kvserver --config config.yaml --node-id 1 --port 22000
./target/debug/kvserver --config config.yaml --node-id 2 --port 23000
./target/debug/kvserver --config config.yaml --node-id 3 --port 24000
./target/debug/kvserver --config config.yaml --node-id 4 --port 25000
```

Using the client to connect to the leader then Set/Get/Delete values.
Not that leader discovery has not been implemented yet. So you will need to watch the log to identifier the leader.

```
./target/debug/kvserver-client --port 23000 
```

[<img alt="https://github.com/styvane/raft/actions?query=branch%3Amain" src="https://img.shields.io/github/workflow/status/styvane/raft/CI/main">](https://github.com/styvane/raft/actions?query=branch%3Amain)
