package com.wisc.raft.subcluster.service;

import com.wisc.raft.subcluster.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.RaftServiceGrpc;
import com.wisc.raft.subcluster.server.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RaftConsensusService extends RaftServiceGrpc.RaftServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(RaftConsensusService.class);

    Server server;

    public RaftConsensusService(Server server) {
        this.server = server;
    }

    @Override
    public void requestVotes(Raft.RequestVote request, StreamObserver<Raft.ResponseVote> responseObserver) {
        logger.debug("[RequestVoteService] Inside Request Vote Service Call for :: " + request.getCandidateId());
        Raft.ResponseVote.Builder responseBuilder = Raft.ResponseVote.newBuilder()
                .setGrant(false)
                .setTerm(this.server.getState().getCurrentTerm());

        this.server.getLock().lock();
        try {
            if (request.getCandidateId().equals(this.server.getState().getNodeId())) {
                logger.debug("[RequestVoteService] Oops!! Candidate cant be voting itself or something term wise wrong!");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            if (request.getTerm() <= this.server.getState().getCurrentTerm()
                    && this.server.getState().getNodeType().equals(Role.LEADER)) {
                logger.debug("[RequestVoteService] Oops!! Candidate cant be voting as you are already a leader");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            long lastLogTerm = this.server.getState().getLastApplied() == -1 ? -1 : this.server.getState().getEntries().get((int)this.server.getState().getLastApplied()).getTerm();

            long lastLogIndex = this.server.getState().getLastApplied() == 0 ? -1 : this.server.getState().getLastApplied();

            if(request.getLeaderLastAppliedTerm()  < lastLogTerm || request.getLeaderLastAppliedIndex() < this.server.getState().getLastApplied()){
                logger.debug("[RequestVoteService] You have bigger Term or more entries than the pot. leader " + " Leader Term : " + request.getLeaderLastAppliedTerm() + " My Term : " + lastLogTerm);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            logger.debug("[RequestVoteService] Current Role :: " + this.server.getState().getNodeType());
            if (request.getTerm() > this.server.getState().getCurrentTerm()) {
                logger.debug("[RequestVoteService] Candidate term :: " + request.getTerm());
                logger.debug("[RequestVoteService] Follower term :: " + this.server.getState().getCurrentTerm());

                // set the current term to the candidates term
                this.server.getState().setCurrentTerm(request.getTerm());
                this.server.getState().setVotedFor(request.getCandidateId());

                logger.debug("[RequestVoteService] I voted for ::" + this.server.getState().getVotedFor());
                this.server.getState().setNodeType(Role.FOLLOWER);  // @TODO if this guy is voting ideally he should step down
                responseBuilder.setGrant(true);
                responseBuilder.setTerm(this.server.getState().getCurrentTerm());
                responseBuilder.setCandidateLastLogIndex(this.server.getState().getLastLogIndex());
                responseBuilder.setCandidateLastAppliedLogIndex(this.server.getState().getLastApplied());
                logger.debug("[RequestVoteService] Candidate Last Applied Index : "+this.server.getState().getLastApplied()+" Last log index : " + this.server.getState().getLastLogIndex());
                logger.debug("[RequestVoteService] Successfuly voted! Current Leader :: " +
                        this.server.getState().getVotedFor() + " Current Node Type ::" +
                        this.server.getState().getNodeType());

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            } else {
                logger.debug("[RequestVoteService] My current term is more than asked!!");
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
        } finally {
            this.server.getLock().unlock();
        }
    }

    @Override
    public void appendEntries(Raft.AppendEntriesRequest request, StreamObserver<Raft.AppendEntriesResponse> responseObserver) {
        logger.debug("[AppendEntriesService] : Follower and my terms is : " + this.server.getState().getCurrentTerm());
        long leaderTerm = request.getTerm();
        long lastIndex = request.getLastAppendedLogIndex();
        String serverID = request.getLeaderId();
        long lastTerm = request.getLastAppendedLogTerm();
        long commitIndex = request.getCommitIndex();
        int indexTracked = (int) request.getIndexTracked();
        Raft.AppendEntriesResponse.Builder responseBuilder = Raft.AppendEntriesResponse.newBuilder();
        this.server.getState().setHeartbeatTrackerTime(System.currentTimeMillis());
        this.server.getLock().lock();
        try {

            if (leaderTerm < this.server.getState().getCurrentTerm()) {
                if(commitIndex < this.server.getState().getCommitIndex()) {
                    logger.info("[AppendEntriesService] Candidate has bigger term and commit index!");
                    responseBuilder = responseBuilder.setSuccess(false).setTerm(this.server.getState().getCurrentTerm()); // What ??
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }
                else {
                    logger.info("[AppendEntriesService] Stepping down as follower since my term is mismatch");
                    this.server.getState().setNodeType(Role.FOLLOWER);
                    this.server.getState().setVotedFor(request.getLeaderId());
                    this.server.getState().setCurrentTerm(leaderTerm);
                }
            }

            if (leaderTerm >= this.server.getState().getCurrentTerm()) {
                logger.debug("[AppendEntriesService] Ensuring current node is follower since term is lesser");
                this.server.getState().setNodeType(Role.FOLLOWER);
                this.server.getState().setVotedFor(request.getLeaderId());
                this.server.getState().setCurrentTerm(leaderTerm);
                responseBuilder.setTerm(this.server.getState().getCurrentTerm());

                if (request.getLastAppendedLogIndex() > this.server.getState().getEntries().size()) {
                    logger.debug("[AppendEntriesService] Request LastAppendLog index : " +request.getLastAppendedLogIndex()+" is more than current log entry size : "+ this.server.getState().getEntries().size());
                    responseBuilder = responseBuilder.setSuccess(false).setTerm(-3);
                    if( this.server.getState().getEntries().size() == 0){
                        responseBuilder.setLastMatchIndex(-1);
                    }
                    else{
                        responseBuilder.setLastMatchIndex(this.server.getState().getEntries().size()-1);
                    }
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }

                List<Raft.LogEntry> currentEntries = this.server.getState().getEntries();
                List<Raft.LogEntry> leaderEntries = request.getEntriesList();

                if(this.server.getState().getCommitIndex() != 0 && this.server.getState().getCommitIndex() > request.getLastAppliedIndex()){
                    logger.debug("[AppendEntriesService] Rejecting AppendEntries RPC: Commit too ahead in the follower : Sacrilige" );
                    responseBuilder = responseBuilder.setSuccess(false).setTerm(-5);
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;

                }

                if (request.getLastAppendedLogTerm() != -1 && this.server.getState().getLastApplied() != -1 &&
                        this.server.getState().getEntries().size() > request.getLastAppendedLogIndex() &&
                        this.server.getState().getEntries().get((int) this.server.getState().getLastApplied()).getTerm() != request.getLastAppendedLogTerm()) {
                    logger.debug("[AppendEntriesService] Rejecting AppendEntries RPC: terms don't agree " );
                    //rollback by sending one at a time
                    this.server.getState().getEntries().subList((int) this.server.getState().getLastApplied() , this.server.getState().getEntries().size()).clear();
                    this.server.getState().setLastApplied(this.server.getState().getLastApplied()-1);
                    responseBuilder = responseBuilder.setSuccess(false).setTerm(-4); // What ??
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }

                if(this.server.getState().getEntries().size() > (int) request.getLastAppendedLogIndex() + 1) {
                    logger.debug("[AppendEntriesService] Removing entries from "+ request.getLastAppendedLogIndex() + " to "+this.server.getState().getEntries().size());
                    this.server.getState().getEntries().subList((int) request.getLastAppendedLogIndex() + 1, this.server.getState().getEntries().size()).clear();
                }
                this.server.getState().getEntries().addAll(leaderEntries);
                this.server.getState().setLastApplied(request.getLastAppliedIndex());
                if(request.getLastAppliedIndex() > this.server.getState().getEntries().size()) {
                    logger.debug("[AppendEntriesService] Oops!! Internesting scneario where Leader's Last Applied is more than your size! "+
                            request.getLastAppliedIndex() + " : "+ this.server.getState().getEntries().size());
                }
                this.server.getState().setLastApplied(request.getLastAppliedIndex());
                long index = this.server.getState().getEntries().size() - 1;
                this.server.getState().setLastLogIndex(index);
                this.server.getState().setLastLeaderCommitIndex(request.getCommitIndex());
                responseBuilder.setLastMatchIndex(index);
                responseBuilder.setLastMatchTerm(index == -1 ? 0 : this.server.getState().getEntries().get((int) index).getTerm()); //Check this
                responseBuilder.setSuccess(true);

                //Do other stuff
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }
        } finally {
            logger.debug("[AppendEntriesService] Log Entries has " + this.server.getState().getEntries().size() + " entries");
            this.server.getState().getEntries().stream().forEach(le ->
                    logger.debug(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue()));
            this.server.getLock().unlock();
        }
    }

}
