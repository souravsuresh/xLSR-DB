# xLSR-DB: eXtensible Large Scale Replicated DataBase solution

In this project, we propose building a replicated distributed database. The basic idea will be to first build a simple server. We choose to build a replicated database from existing libraries, like LevelDB or Memcached or SQLite. 

Once server is buily, we will use a consensus algorithm to build a replicated database service. In this project, we will build this approach leveraging RAFT [1]. We'll then demonstrate how our approach handles failures, and aspects of its performance and scalability. 

Report can be found at : [xLSR-DB Report](xLSR-DB.pdf)

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

Subcluster:

```
mvn exec:java -Dexec.mainClass="com.wisc.raft.subcluster.RaftServer" -Dexec.args="1 8082 0_localhost_8081 1_localhost_8082 2_localhost_8083"
```

LoadBalancer:

```
mvn exec:java -Dexec.mainClass="com.wisc.raft.loadbalancer.RaftServer" -Dexec.args="1 8082 0_localhost_8081 1_localhost_8082 2_localhost_8083"
```

Exaplanation of params:
-----------------------------
args[0] = current Server ID
args[1] = current Server Port
args[2] = space seperated id_hostname_port covering all node details in cluster (including the current one).
```

AutoScaler

```
mvn exec:java -Dexec.mainClass="com.wisc.raft.autoscaler.AutoScalerMain" 
```

3. [Optional] We can run client simulation script to simulate the writes.
```
mvn exec:java -Dexec.mainClass="client.com.wisc.raft.subcluster.ClientMachine" -Dexec.args="localhost 9091 100000 _ 50000 500 write"
```

```
Explanation of params:
-----------------------------
args[0] = server hostname (can be any node in cluster <leader/follower>)
args[1] = server port
args[2] = number of operations
args[3] = client hostname
args[4] = client port
args[6] = operations (write/ read/ writeread)
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
