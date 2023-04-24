

#### Persistent state on all servers:

- currentTerm:  latest term server has seen (initialized to 0 on first boot, increases monotonically)
- votedFor: candidateId that received vote in current term (or null if none)
- log[]:  log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)


#### Volatile state on all servers:
- commitIndex: index of highest log entry known to be
- committed: (initialized to 0, increases monotonically)
- lastApplied: index of highest log entry applied to state
- machine: (initialized to 0, increases monotonically)


#### Volatile state on leaders:
(Reinitialized after election)

- nextIndex[]: for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
- matchIndex[]: for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
