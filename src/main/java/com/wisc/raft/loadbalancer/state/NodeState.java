package com.wisc.raft.loadbalancer.state;

import com.wisc.raft.loadbalancer.constants.Role;
import com.wisc.raft.proto.Raft;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class NodeState {

    private String nodeId;
    private long currentTerm;
    private String votedFor;
    private String nodeAddress;
    private List<Raft.LogEntry> entries;
    private List<Raft.LogEntry> snapshot;

    // load balancer related states
    private ConcurrentHashMap<Integer, List<Raft.ServerConnect>> clusterDetails;       // cluster id to cluster details [localhost:8080, locahost:8081,...]
    private List<List<Raft.LogEntry>> loadBalancerEntries;
    private List<List<Raft.LogEntry>> loadBalancerSnapshot;
    private List<List<Boolean>> loadBalancerProcessStatus;      // status of that key within the process list


    private Role nodeType;

    // volatile state on all servers
    private long commitIndex;    // index of highest log entry known to be commited
    private long lastApplied;    // index of highest log entry applied to state machine
    private long lastLogIndex;  // index of last log entry

    // load balancer related variables
    private List<Long> lastLoadBalancerProcessed;                     // indicates last completely processed index
    private List<Long> lastLoadBalancerLogIndex;          // indicates last pointer to the list of log index
    private List<Long> lastLoadBalancerCommitIndex;       // indicates last commit (movement from LB log -> Raft log) snapshot


    //volatile state on leaders
    private List<Integer> nextIndex;    // for each server, index of next log entry to send to a particular server
    // initialized to leader logIndex+1
    private List<Integer> matchIndex;    // for each server, index of highest log entry known to be replicated on that particular server
    // initialized to 0
    private long leaderTerm;

    private long lastLeaderCommitIndex;
    private long heartbeatTrackerTime;
    private int totalVotes;
    private String leaderId;

    private int clusterId;

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }


    public NodeState(String nodeId) {
        this.currentTerm = 0;
        this.votedFor = null;
        this.entries = new ArrayList<>();
        this.snapshot = new ArrayList<>();
        totalVotes = 0;
        this.commitIndex = -1;
        this.lastApplied = -1;

        this.nextIndex = new ArrayList<>();
        this.matchIndex = new ArrayList<>();

        this.nodeType = Role.FOLLOWER;
        this.leaderTerm = 0;
        this.nodeId = nodeId;

        this.heartbeatTrackerTime = 0;
        this.lastLogIndex = 0;
        this.lastLeaderCommitIndex = -1;

        this.lastLoadBalancerLogIndex = new ArrayList<>();          // initialize new elements with 0
        this.lastLoadBalancerProcessed = new ArrayList<>();                     // initialize new elements with -1
        this.lastLoadBalancerCommitIndex = new ArrayList<>();       // initialize new elements with -1
        this.loadBalancerEntries = new ArrayList<>();
        this.loadBalancerSnapshot = new ArrayList<>();
        this.loadBalancerProcessStatus = new ArrayList<>();
        this.clusterDetails = new ConcurrentHashMap<>();                      // @TODO might need to initlaize with initial set of clusters
    }
}
