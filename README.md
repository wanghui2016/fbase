# FBASE
F means flat，fast，and flexible

# data model

Namespace -> Table -> Range -> Row (RowKey -> AttrName -> AttrValue)

A namespace usually contains multiple tables and a table can consist of unlimited number of rows sorted by keys. 

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

And FBASE adopts option B.

# rebalancing

data/load is rebalanced via replica adjustment coordinated by the master



