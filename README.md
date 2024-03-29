raft
====

This is *another* attempt implementing the [Raft](https://raft.github.io/raft.pdf) consensus algorithm.

It's not indented to be used in production.

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


Installation
------------

```
cargo install --bins --path kvserver
```

Run
---

To run this, we need a configuration see [example](config.example.yaml).

We will then start the following processes. The number of processes depends on the configuration. 

```
kvserver --config config.yaml --node-id 0 --port 21000
kvserver --config config.yaml --node-id 1 --port 22000
kvserver --config config.yaml --node-id 2 --port 23000
kvserver --config config.yaml --node-id 3 --port 24000
kvserver --config config.yaml --node-id 4 --port 25000
```

Using the client to connect to the leader, then Set/Get/Delete values.
I did not implement leader discovery, so you will need to watch the log to identifier the leader.

```
kvserver-client --port 23000 
```
