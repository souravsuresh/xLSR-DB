# xLSR-DB: eXtensible Large Scale Replicated DataBase solution

This research project focuses on designing a extensible large Scale storage solution leveraging Raft consensus algorithm to build a cloud native replicated database system using LevelDB as the backend datastore. The proposed algorithm is an adaptive version that has flexibility to autoscale the system and also batches requests to improve overall system throughput. Communication between nodes is achieved using gRPC. The main objective of this study is to demonstrate the effectiveness of build a large scale storage systems which work efficiently in Cloud Environments where the scalability is one of the huge factor.

### Installation
----
1. Build the project
```
mvn clean install
```

2. Spin up the servers. (We can change the port numbers and hostnames accrodingly).

    **Subcluster:**

    ```
    mvn exec:java -Dexec.mainClass="com.wisc.raft.subcluster.RaftServer" -Dexec.args="1 8082 0_localhost_8081 1_localhost_8082 2_localhost_8083"
    ```

    **LoadBalancer:**

    ```
    mvn exec:java -Dexec.mainClass="com.wisc.raft.loadbalancer.RaftServer" -Dexec.args="1 8082 0_localhost_8081 1_localhost_8082 2_localhost_8083"
    ```
    ```
    Exaplanation of params:
    -----------------------------
    args[0] = current Server ID
    args[1] = current Server Port
    args[2] = space seperated id_hostname_port covering all node details in cluster (including the current one).
    ```

    **AutoScaler:**

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

*Note: This is developed as extension over our base version of [Replicated Database using Raft](https://github.com/souravsuresh/ReplicatedDB)*

<!-- 
Report can be found at : [xLSR-DB Report](xLSR-DB.pdf) -->