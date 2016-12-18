# fdb

Flat Data Store

# data model

namespace -> table -> row (key-value)

# concepts

Master

Node

Disk

Range

node -> disk -> range

# replication

Range works as the replication unit

Raft consensus

# split

Option A: a split is also a replicated write operation which creates a new range within the original disk, so it is triggered by the range leader without coordination by the master

Option B: a split is not local to the source node but migrates one or two new shards to other nodes so coordinated by the master; each range has a state machine - Creating, Active, Frozen, Removing, et al.

# rebalancing

data/load is rebalanced via replica adjustment coordinated by the master



