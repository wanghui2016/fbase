# FBASE
A distrubuted storage system. A fbase cluster consists of gateway-server, master-server, data-server and web-manager. 

features
--------
Support SQL syntax, users can access with sql client directly.
Database supports row-level expire time.
Dynamic table scheme, add or rename column is allowed.
Table can be created pre-sharding.
Data global sorted, and strong consistency.
Storage can auto scale out, auto failover, auto rebalance.

components
----------
Master-server is a metadata service, can be configed with 3-replica for more high available. Users create databases ,tables and columns are saved here. It is a controller of data-server, schedules ranges in data-server for scale out, failover and rebalance. It is a router center of gateway-server, all tables topology are saved here.
users can access table with simple sql sytax by gateway-server, and it is also provide restful interface. 
data-server is a LSM kv storage, using raft replication.

how to build a cluster
----------------------
A cluster can be consisted of master-server 1 at least (3 replica is suggested), gateway-server 1 at least, and data-server *3 at least.
Master-server and data-server needs raft group. Gateway-server usually uses a domain, so add at anytime when needed.

To make a master-server replica group, in master-server config file, 'cluster.infos' is needed:

	% cluster.infos = ${id}-${ip1}-${manage_port}-${rpc_port}-${heartbeat_port}-${replica_port}:...*3

'cluster.infos' represents the master-server replica group members, manage_port for web manage server, rpc_port for communicating with gateway-server and data-server, hearbeat_port and replica_port for using raft replication itself.
Then master-server can work together.

In data-server config file, the master-server address is needed:

	% master.addrs = ${master_server_ip}:${master_server_manage_port}:${master_server_rpc_port};...*3

'master.addrs' points to the master-server addresses of this cluster, in order to data-server can pull reported stats, push master task.
There is also 'master.addrs' in gateway-server config file, for gateway-server subsribe route of tables.


