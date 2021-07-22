raft
====

[<img alt="https://github.com/styvane/raft/actions?query=branch%3Amain" src="https://img.shields.io/github/workflow/status/styvane/raft/CI/main">](https://github.com/styvane/raft/actions?query=branch%3Amain)


This is *another* attempt implementing the [Raft](https://raft.github.io/raft.pdf) consensus algorithm.

It's not production code and does not support the persistence of the key/value store.

I did not implement the following ideas in the paper:

- Persistency(all states are volatiles).
- Cluster membership changes
- Log compaction

TODO
----

- Leader discovery and confirmation
- Append only *write* operation entry
- Persist log and non-volatiles states.
- Graceful shutdown


Run
---

To run this, we need a configuration see [example](config.example.yaml).

We will then start the following processes. The number of processes depends on the configuration. 

```
./target/debug/kvserver --config config.yaml --node-id 0 --port 21000
./target/debug/kvserver --config config.yaml --node-id 1 --port 22000
./target/debug/kvserver --config config.yaml --node-id 2 --port 23000
./target/debug/kvserver --config config.yaml --node-id 3 --port 24000
./target/debug/kvserver --config config.yaml --node-id 4 --port 25000
```

Using the client to connect to the leader, then Set/Get/Delete values.
I did not implement leader discovery, so you will need to watch the log to identifier the leader.

```
./target/debug/kvserver-client --port 23000 
```
