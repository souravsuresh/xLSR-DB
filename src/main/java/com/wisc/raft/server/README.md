Server
----

Main driver for the given Raft Node/ server.


---
### Candidate
----
 1. Increment currentTerm
 2. vote for self
 3. Reset eleciton timeout
 4. Send RequestVoteReq RPCs to all other servers, wait for either:
    a.Votes received from majority of servers: become leader
    b. AppendEntries RPC received from new leader: step down
    c. Election timeout elaspse without election resolution: Increment term, start new election
    d. Discover higher term: step down

---
### Follower
---
1. Respond to RPCs from candicates and leaders
2. Convert to candicate if electon timeout elapses without either:
 a. Receiving valid AppendEntries RPC, or
 b. Granting vote to candidate

---
### Leader
----
 1. Initialize nextIndex for each to last log index + 1;
 2. Send inital empty AppendEntries RPCs(heartbeat) to each follower; repeat during idle periods to prevent election timeout;
 3. Accept commands from clients, append new entries to local log;
 4. Whenever last log index >= nextIndex for a follower, send AppendEntries RPC with log entries starting at nextIndex, update nextIndex if successful;
 5. If AppendEntries fails because of log inconsistency, decrement nextIndex and retry;
 6. Mark log entries committed if stored on a majority of servers and at least one entry from current term is stored on a majority of servers;
 7. Step down if currentTerm changes;

---
### RPC Implementations
----
1. RequestVote 
    - Reply false if term < currentTerm
    - If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote


2. AppendEntries
    - Reply false if term < currentTerm
    - Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm 
    - If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it 
    - Append any new entries not already in the log
    - If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)


