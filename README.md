# fdb

Flat Data Store

# concepts

Cluster Manager (CM)

Node

Local Storage Engine per Disk

Range

node ->  -> range

# replication

Range works as the replication unit

Raft consensus

# splitting

splitting as a write operation to be replicated to create a new range within the original disk

* rebalancing

data/load is rebalanced via replica adjustment coordinated by CM



