package com.wisc.raft.subcluster.state;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.constants.Role;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class NodeState {

    private String nodeId;
    private long currentTerm;
    private String votedFor;       
    private String nodeAddress;
    private List<Raft.LogEntry> entries;
    private List<Raft.LogEntry> snapshot;
    private Role nodeType;
    
    // volatile state on all servers
    private long commitIndex;    // index of highest log entry known to be commited 
    private long lastApplied;    // index of highest log entry applied to state machine
    private long lastLogIndex;  // index of last log entry

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
    }
}