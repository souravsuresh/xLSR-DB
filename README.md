# ReplicatedDB

In this project, you’ll be building a replicated distributed database. You’ll apply knowledge you have learned in class to build a system that works despite failure of servers. Fun!

The basic idea will be to first build a simple server. Here, you have some options. The most basic is to build a simple key/value storage system (details below). But, you can also instead choose to build a replicated database from existing libraries, like LevelDB or Memcached or SQLite. 

Once you have built a server, you will use a consensus algorithm to build a replicated database service. In this project, you will build this approach by hand, choosing either Raft [1] or Paxos [2] or Viewstamped Replication [3]; these are all pretty similar approaches [4, 5].

You’ll then demonstrate how your approach handles failures, and aspects of its performance. 

### Details
-------
The first thing to do is to build a simple server. The simplest path is likely a get/put key-value storage system. The interface should be simple, such as “put(key, value)” and “get(key)”; the server can simply keep these values in memory.

Your server should take, as input, a simple RPC interface which sends over get/put commands. Don’t worry about security for this project; as such, anyone will be able to connect to the database and run queries against it.

Once you have a basic client/server setup working, you will layer in a consensus approach to build a strongly consistent [6] replicated database. Very likely, this will include having the servers, when they start, elect a leader. A client will then connect to that leader (or, if connected to another server, be redirected to the leader) to send a request to the service; the servers will then follow the protocol of choice to perform the action in the style of a replicated state machine; finally, an answer to the query will be sent back to the clients. Exact details of how exactly you do all of these things is left up to you.

Options: Instead of building your own simple server, build one using LevelDBLinks to an external site., RocksDBLinks to an external site., SQLiteLinks to an external site., MemcachedLinks to an external site., or something similar. This provides an extra challenge! But, you may be rewarded: Most impressive project will win the “Best Project” prize - fame, glory, and a T-shirt!

### Demonstration
-----
A working project will demonstrate successful basic functionality under no failures, and successful operation under some number of server failures. A successful project will also demonstrate some of the performance characteristics of the system that was built; a good example of performance evaluation is found in the EPaxos paper [7]. Please note down your design decisions and justify them as well. 

Testing correctness is naturally difficult for Raft, Paxos, and the like. Please consider how you will do so carefully, and make this discussion part of your final presentation. 

A more advanced project will implement features such as membership change; however, this is not required and probably only needed for those shooting for the “best project” prize.

### Installation
----
1. Build the project
```mvn clean install```

2. Spin up the servers. (We can change the port numbers and hostnames accrodingly).
```
mvn exec:java -Dexec.mainClass="com.wisc.raft.RaftServer" -Dexec.args="1 8082 0_localhost_8081 1_localhost_8082 2_localhost_8083"
```
```
Exaplanation of params:
-----------------------------
args[0] = current Server ID
args[1] = current Server Port
args[2] = space seperated id_hostname_port covering all node details in cluster (including the current one).
```

3. [Optional] We can run client simulation script to simulate the writes.
```
mvn exec:java -Dexec.mainClass="com.wisc.raft.client.ClientMachine" -Dexec.args="localhost 8082 1000 localhost 9000"
```
```
Explanation of params:
-----------------------------
args[0] = server hostname (can be any node in cluster <leader/follower>)
args[1] = server port
args[2] = number of operation
args[3] = client hostname
args[4] = client port
```



### References
-----
[1] https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14Links to an external site.

[2] https://lamport.azurewebsites.net/pubs/paxos-simple.pdfLinks to an external site.

[3] https://pmg.csail.mit.edu/papers/vr-revisited.pdfLinks to an external site.

[4] https://ellismichael.com/blog/2017/02/28/raft-equivalency/Links to an external site.

[5] http://mpaxos.com/pub/raft-paxos.pdfLinks to an external site.

[6] https://en.wikipedia.org/wiki/Strong_consistencyLinks to an external site.

[7] https://www.usenix.org/system/files/nsdi21-tollman.pdfLinks to an external site.