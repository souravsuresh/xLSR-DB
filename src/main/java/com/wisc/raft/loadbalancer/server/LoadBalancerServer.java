package com.wisc.raft.loadbalancer.server;

import com.wisc.raft.autoscaler.AutoScaleService;
import com.wisc.raft.loadbalancer.constants.Role;
import com.wisc.raft.loadbalancer.dto.ReadLBObject;
import com.wisc.raft.loadbalancer.service.LoadBalancerDatabase;
import com.wisc.raft.loadbalancer.service.LoadBalancerLiveLinessService;
import com.wisc.raft.loadbalancer.service.RaftConsensusService;
import com.wisc.raft.loadbalancer.state.NodeState;
import com.wisc.raft.proto.*;
import com.wisc.raft.subcluster.service.LoadBalancerService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Setter
public class LoadBalancerServer {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerServer.class);

    private static final Random random = new Random();
    private static final long ELECTION_TIMEOUT_INTERVAL = 100;  //100ms
    private static final long HEARTBEAT_TIMEOUT_INTERVAL = 80;  //80ms
    private static final long MAX_REQUEST_RETRY = 3;  // retry requests for 3 times

    private static long termIncreaseInterval = 3;  // retry requests for 3 times


    private static long logAppendRetries = 0;
    private static long rejectionRetries = 0;

    private NodeState state;
    private List<Raft.ServerConnect> cluster;

    private LoadBalancerLiveLinessService loadBalancerLiveLinessService;
    private List<Raft.ServerConnect> subClusterList;

    //Threading
    private ScheduledExecutorService electionExecutorService;
    private ScheduledExecutorService heartbeatExecutorService;
    private ScheduledExecutorService commitSchedulerService;
    private ScheduledExecutorService replySchedulerService;
    private ScheduledExecutorService liveLinessSchedulerService;
    private ScheduledExecutorService loadBalancerProcessorSchedulerService;
    private ScheduledExecutorService cleanUpVersionService;

    private AutoScaleService autoScaleService;

    private ScheduledFuture electionScheduler;
    private ScheduledFuture heartBeatScheduler;
    private ScheduledFuture commitScheduler;
    private ScheduledFuture replyScheduler;
    private ScheduledFuture loadBalancerProcessorScheduler;

    private static long lbRejectionRetries = 0;

    private ThreadPoolExecutor electionExecutor;
    private ThreadPoolExecutor heartBeatExecutor;
    private ThreadPoolExecutor commitExecutor;
    private ThreadPoolExecutor replyExecutor;
    private ThreadPoolExecutor liveLinessExecutor;
    private ThreadPoolExecutor loadBalancerProcessorExecutor;


    private ExecutorService appendEntriesExecutor;

    private Lock lock;
    private LoadBalancerDatabase db;

    private ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStub;
    private ConcurrentHashMap<String, Pair<String, Boolean>> persistentStore;

    private RaftConsensusService raftConsensusService;

    //TODO ADD THIS CONSTRUCTOR
    private Raft.Endpoint autoScaleEndpoint;

    private PriorityQueue<Pair<Long, Double>> subClusterUtilizationQueue;


    String autoScalerHostName;
    int autoScalerPortNumber ;
    public LoadBalancerServer(String nodeId, LoadBalancerDatabase db, int autoScalerPortNumber, String autoScalerHostName) {
        this.state = new NodeState(nodeId);
        if (Objects.isNull(db)) {
            logger.error("Level-DB is not setup properly! (Solution: Check if there is leveldb_<> file as mentioned in this log and delete/ just rerun!)");
        }
        this.db = db;
        this.autoScalerPortNumber = autoScalerPortNumber;
        this.autoScalerHostName = autoScalerHostName;
        this.subClusterList = new ArrayList<>();
        this.loadBalancerLiveLinessService = new LoadBalancerLiveLinessService(new ConcurrentHashMap<>(), 3);
    }

    public void init() {
        logger.info("Starting the services at :: " + System.currentTimeMillis());

        lock = new ReentrantLock();
        //db = new Database();
        List<Integer> matchIndex = new ArrayList<>();
        List<Integer> nextIndex = new ArrayList<>();
        logger.info("Cluster size : " + cluster.size());
        cluster.stream().forEach(serv -> {
            matchIndex.add(-1);
            nextIndex.add(0);
        });
        persistentStore = new ConcurrentHashMap<>();
        this.state.setNextIndex(nextIndex);
        this.state.setMatchIndex(matchIndex);

        Runnable initiateElectionRPCRunnable = () -> initiateElectionRPC();
        Runnable initiateHeartbeatRPCRunnable = () -> initiateHeartbeatRPC();
        Runnable initiateCommitRPCRunnable = () -> initiateCommitScheduleRPC();
        //Runnable replyClientExecutorRunnable = () -> initiateReplyScheduleRPC();
        Runnable sendLiveLinessProbesRunnable = () -> initiateLiveLinessProbesRPC();
        Runnable cleanUpVersionsRunnable = () -> initiateCleanUpVersions();
        Runnable loadBalancerProcessorRunnable = () -> initiateLBProcessor();

        this.electionExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.heartBeatExecutor = new ThreadPoolExecutor(cluster.size(), cluster.size(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.liveLinessExecutor = new ThreadPoolExecutor(10, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());




        electionExecutorService = Executors.newSingleThreadScheduledExecutor();
        liveLinessSchedulerService = Executors.newSingleThreadScheduledExecutor();
        cleanUpVersionService = Executors.newSingleThreadScheduledExecutor();
        cleanUpVersionService.scheduleAtFixedRate(cleanUpVersionsRunnable,1,1,TimeUnit.HOURS);
        liveLinessSchedulerService.scheduleAtFixedRate(sendLiveLinessProbesRunnable, 0, 10, TimeUnit.SECONDS);
        electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 2, 14, TimeUnit.SECONDS);

//        electionScheduler = electionExecutorService.scheduleAtFixedRate(initiateElectionRPCRunnable, 1L, (long) (100 + random.nextDouble() * 100), TimeUnit.MILLISECONDS);
        heartbeatExecutorService = Executors.newSingleThreadScheduledExecutor();
        heartBeatScheduler = heartbeatExecutorService.scheduleAtFixedRate(initiateHeartbeatRPCRunnable, 1, 7, TimeUnit.SECONDS);
//        heartBeatScheduler = heartbeatExecutorService.scheduleAtFixedRate(initiateHeartbeatRPCRunnable, 1500, 80, TimeUnit.MILLISECONDS);
        loadBalancerProcessorSchedulerService = Executors.newSingleThreadScheduledExecutor();
        loadBalancerProcessorScheduler = loadBalancerProcessorSchedulerService.scheduleAtFixedRate(loadBalancerProcessorRunnable, 2, 7, TimeUnit.SECONDS);

        commitSchedulerService = Executors.newSingleThreadScheduledExecutor();
        commitScheduler = commitSchedulerService.scheduleAtFixedRate(initiateCommitRPCRunnable, 2, 10, TimeUnit.SECONDS);

    }

    private void checkAndCommitFromLoadBalancerProcessor(int clusterIndex) {
        lock.lock();
        try {
            logger.debug("[checkAndCommitFromLoadBalancerProcessor] Indexes commit2 :: "+ this.state.getLastLoadBalancerCommitIndex().get(clusterIndex) + " :: processed"
                        +this.state.getLastLoadBalancerProcessed().get(clusterIndex) + " :: Last Log index"
                        + this.state.getLastLoadBalancerLogIndex().get(clusterIndex));
            if(this.state.getLoadBalancerEntries().get(clusterIndex).size() == 0) {
                logger.info("[checkAndCommitFromLoadBalancerProcessor] Nothing to commit from LB to DB");
                return;
            }
            if(this.state.getLastLoadBalancerCommitIndex().get(clusterIndex) >= this.state.getLastLoadBalancerProcessed().get(clusterIndex)) {
                logger.info("[checkAndCommitFromLoadBalancerProcessor] Last log index and commit index are same. Nothing to commit");
                return;
            }

            for(long i = this.state.getLastLoadBalancerCommitIndex().get(clusterIndex) + 1L;
                        i < this.state.getLastLoadBalancerProcessed().get(clusterIndex); ++i){
                String key = this.state.getLoadBalancerEntries().get(clusterIndex).get((int) i).getCommand().getKey();
                int version = this.state.getLoadBalancerEntries().get(clusterIndex).get((int) i).getCommand().getVersion();
                String value = clusterIndex + "_v1";
                Raft.LogEntry le = Raft.LogEntry.newBuilder()
                                        .setCommand(Raft.Command.newBuilder()
                                                    .setValue(value)
                                                    .setKey(key).setVersion(version).build())
                                        .setTerm(this.state.getCurrentTerm())
                                        .setIndex("METADATA")
                                        .setClusterId(clusterIndex)
                                        .setRequestId(String.valueOf(UUID.randomUUID())).build();
                // Adding to snapshot so that LB gets further consensus
                this.state.getSnapshot().add(le);
                logger.debug("[checkAndCommitFromLoadBalancerProcessor] "+ le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue());
            }
            this.state.getLastLoadBalancerCommitIndex().set(clusterIndex, this.state.getLastLoadBalancerProcessed().get(clusterIndex) - 1);
        } finally {
            lock.unlock();
        }
    }

    private void initiateCommitScheduleRPC(){
        lock.lock();
        try {
            if (this.state.getNodeType() == Role.LEADER) {
                for(int i = 0; i < this.subClusterList.size(); ++i) {
                    checkAndCommitFromLoadBalancerProcessor(i);
                }
                if(this.state.getCommitIndex() <= this.state.getLastApplied() &&  this.state.getCommitIndex() <= this.state.getEntries().size() && this.state.getLastApplied() <=  this.state.getEntries().size()){
                    long index = this.state.getCommitIndex();
                    logger.debug("[CommitSchedule] inside leader commit starting from " + (index + 1) + " to "+ this.state.getLastApplied());
                    for(long i = index+1; i<=this.state.getLastApplied(); i++){
                        int ret = db.commit(this.state.getEntries().get((int) i));
                        if(ret == -1){
                            logger.debug("[CommitSchedule] Failed but no issues");
                        }
                        else{
                            logger.info("[CommitSchedule] Commited successfully :: "+i + " at "+System.currentTimeMillis());
                        }
//                        Pair<String, Boolean> stringBooleanPair = this.persistentStore.get(this.state.getEntries().get((int) i).getRequestId());
//                        this.persistentStore.put(this.state.getEntries().get((int) i).getRequestId(), new Pair<>(stringBooleanPair.getKey(), ret != -1));
                        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
                    }
                }
            }
            else{
                if(this.state.getCommitIndex() > this.state.getLastLeaderCommitIndex()){
                    logger.debug("[CommitSchedule] Your commit index :: " + this.state.getCommitIndex() + " is more than leader commit index :: "+this.state.getLastLeaderCommitIndex());
                    return;
                }
                if(this.state.getCommitIndex() <= this.state.getLastLeaderCommitIndex()  && this.state.getCommitIndex() < this.state.getEntries().size() && this.state.getLastLeaderCommitIndex() < this.state.getEntries().size()){
                    long index = this.state.getCommitIndex();
                    for(long i=index+1;i<=this.state.getLastLeaderCommitIndex();i++){
                        int ret = db.commit(this.state.getEntries().get((int) i));
                        if(ret == -1){
                            logger.debug("[CommitSchedule] Failed but no issues");
                        }
                        else{
                            logger.info("[CommitSchedule] Commited successfully :: "+i+" at "+System.currentTimeMillis());
                        }
//                        Pair<String, Boolean> stringBooleanPair = this.persistentStore.get(this.state.getEntries().get((int) i).getRequestId());
//                        this.persistentStore.put(this.state.getEntries().get((int) i).getRequestId(), new Pair<>(stringBooleanPair.getKey(), ret != -1));
                        this.state.setCommitIndex(this.state.getCommitIndex() + 1);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("[CommitSchedule] Oops got a intresting exception:: "+ex);
            ex.printStackTrace();
        }
        finally {
            lock.unlock();
        }
    }

    private void populateSubclusters() {
        AtomicInteger c = new AtomicInteger();
//        this.subClusterList.stream().forEach(sc -> {
//            Raft.LogEntry le = Raft.LogEntry.newBuilder().setCommand(Raft.Command.newBuilder().setValue(Integer.toString(random.nextInt(10))).setKey(Integer.toString(random.nextInt(10))).build()).setTerm(this.state.getCurrentTerm()).setIndex("UNIT_TEST").build();
//            logger.info("[UNITTEST-populateSubclusters] Adding le : "+le);
//            this.state.getLoadBalancerSnapshot().get(c.getAndIncrement()).add(le);
//        });
        // @TODO :: Also test with preCall method
        Client.Request request = Client.Request.newBuilder().setEndpoint(Client.Endpoint.getDefaultInstance()).setCommandType("UNIT_TEST").setCommandType("WRITE").setValue(random.nextInt(2)).setKey(random.nextInt(5)).build();
        logger.debug("[populatedata] request :: "+request);
        this.preCall(request);
    }
    public void initiateElectionRPC() {
        Raft.RequestVote.Builder requestBuilder = Raft.RequestVote.newBuilder();
        logger.debug("[initiateElectionRPC] Starting election at :: "+ System.currentTimeMillis());
        lock.lock();
        try {
            logger.debug("[initiateElectionRPC] Current time :: " + System.currentTimeMillis() + " HeartBeat timeout time :: " +  (this.state.getHeartbeatTrackerTime() + 5 * 80 * MAX_REQUEST_RETRY));
            // @TODO UNCOMMENT THIS to step down as follower
//            if(this.state.getHeartbeatTrackerTime() != 0 && System.currentTimeMillis() > (this.state.getHeartbeatTrackerTime() +  5 * 80 * MAX_REQUEST_RETRY) ) {
//                logger.info("[initiateElectionRPC] Stepping down as follower at :: "+ System.currentTimeMillis());
//                this.state.setVotedFor(null);
//                this.state.setNodeType(Role.FOLLOWER);
//            }
            if (this.state.getNodeType().equals(Role.LEADER)) {

                logger.debug("[initiateElectionRPC]  Already a leader! So not participating in Election!");
                //@CHECK :: UNCOMMENT THIS TO TEST APPEND ENTRIES SIMULATING CLIENT
//                for(int i = 0 ;i < 1; i++){
//                    this.state.getSnapshot().add();
//                }
//                return;
                this.populateSubclusters();
                return;
            }
            if (!Objects.isNull(this.state.getVotedFor()) && this.state.getNodeType().equals(Role.FOLLOWER)) {
                logger.debug("[initiateElectionRPC]  Already voted ! So not participating in Election! : " + this.state.getVotedFor());
                return;
            }

            logger.debug("[initiateElectionRPC] Starting the voting process");
            this.state.setVotedFor(this.state.getNodeId());
            this.state.setNodeType(Role.CANDIDATE);
            this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
            requestBuilder.setCandidateId(this.state.getNodeId());
            requestBuilder.setTerm(this.state.getCurrentTerm());
            long lastLogTerm = this.state.getLastApplied() == -1 ? -1 : this.state.getEntries().get((int)this.state.getLastApplied()).getTerm();
            requestBuilder.setLeaderLastAppliedTerm(lastLogTerm);
            requestBuilder.setLeaderLastAppliedIndex(this.state.getLastApplied());
            this.state.setTotalVotes(1);
        }
        catch(Exception e){
            logger.error("Initiate RPC before request error " + e);
        }
        finally {
            lock.unlock();
        }
        Raft.RequestVote request = requestBuilder.build();
        cluster.stream().forEach(serv -> {
            if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                this.electionExecutor.submit(() -> requestVote(request, serv));
            }
        });
    }

    private void requestVote(Raft.RequestVote request, Raft.ServerConnect server) {
        Raft.Endpoint endpoint = server.getEndpoint();
        logger.debug("[RequestVoteWrapper] Inside requestVote for endpoint :: " + endpoint.getPort());

        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();
        try {
            logger.debug("[RequestVoteWrapper] Channel state :: " + channel.getState(true) + " :: " + channel.getState(false));
            logger.debug("[RequestVoteWrapper] Sent voting req : " + request.getCandidateId());
            RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
            Raft.ResponseVote responseVote = raftServiceBlockingStub.requestVotes(request);
            if (responseVote.getGrant()) {
                lock.lock();
                try {
                    this.state.setTotalVotes(this.state.getTotalVotes() + 1);
                    if (this.state.getNodeType() != Role.CANDIDATE) {
                        logger.debug("[RequestVoteWrapper] Some other guy took over the leadership!! Or recieved the vote when i am already a leader!");
                    } else if (this.state.getTotalVotes() > cluster.size() / 2) {
                        logger.info("[RequestVoteWrapper] Got the leadership ::  NodeType : " + this.state.getNodeType() + " Votes : " + this.state.getTotalVotes() + " current term: " + this.state.getCurrentTerm());
                        this.state.setHeartbeatTrackerTime(System.currentTimeMillis());
                        this.state.setNodeType(Role.LEADER);
                        logger.info("[RequestVoteWrapper] Got leadership at :: "+System.currentTimeMillis());
                    }
                    logger.debug("[RequestVoteWrapper] Before state of "+server.getEndpoint()+" Match index :: "+ this.state.getMatchIndex().get(server.getServerId()) + " Next Index :: "+ this.state.getNextIndex().get(server.getServerId()));
                    this.state.getNextIndex().set(server.getServerId(), (int) responseVote.getCandidateLastLogIndex() + 1);
                    this.state.getMatchIndex().set(server.getServerId(), (int) responseVote.getCandidateLastLogIndex());
                    logger.debug("[RequestVoteWrapper] After state of "+server.getEndpoint()+" Match index :: "+ this.state.getMatchIndex().get(server.getServerId()) + " Next Index :: "+ this.state.getNextIndex().get(server.getServerId()));
                    logger.debug("[RequestVoteWrapper] Number of Votes : " + this.state.getTotalVotes());
                } finally {
                    lock.unlock();
                }
            } else {
                logger.debug("[RequestVoteWrapper] Not granted by :: " + endpoint.getPort() + " Response :: " + responseVote.getTerm() + " Current :: " + this.state.getCurrentTerm());
            }
        }catch (Exception ex) {
            logger.debug("[RequestVoteWrapper] Server might not be up!! "+ ex);
        } finally{
            channel.shutdown();
        }


    }

    public void initiateHeartbeatRPC() {
        try {
            logger.debug("[RaftService] Log Entries has " + this.state.getEntries().size() + " entries" + " Jelp: " + logAppendRetries);
            if (!this.state.getNodeType().equals(Role.LEADER)) {
                logger.debug("[initiateHeartbeatRPC] Not a leader! So not participating in HeartBeat!");
                return;
            }
            termIncreaseInterval++;
            lock.lock();
            if (termIncreaseInterval > cluster.size() * 2 * 1000) {
                this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
                termIncreaseInterval = 0;
            }
            lock.unlock();
            logger.debug(this.state.getLastApplied() + " getLastApplied :  " +  this.state.getLastLogIndex() + " : getLastLogIndex");

            // if getLastLogIndex is 0 then its a initial case where currnet node is starting fresh
            if (this.state.getLastLogIndex() > 0 && this.state.getLastApplied() < this.state.getLastLogIndex()) {
                // check for majority now!
                int i = 0, majority = 1;
                for(; i < cluster.size();++i) {
                    int serverInd = cluster.get(i).getServerId();
                    if(serverInd != Integer.parseInt(this.state.getNodeId())
                            && this.state.getMatchIndex().get(serverInd) == this.state.getLastLogIndex()) {
                        majority += 1;
                    }
                    logger.debug("Server index :: "+ serverInd + " has match index :: "+this.state.getMatchIndex());
                }
                if (majority <= cluster.size()/2) {
                    logger.debug("[initiateHeartbeatRPC] Retrying the log append retires!! : " + logAppendRetries);
                    logAppendRetries++;
                } else {
                    logger.debug("[initiateHeartbeatRPC] Resetting everything : " + logAppendRetries);
                    logger.info("Got majority for entires from " + this.state.getLastApplied() + " to  "+(this.state.getLastLogIndex()+1) + "at "+System.currentTimeMillis());
                    rejectionRetries = 0;
                    logAppendRetries = 0;
                }
                if(logAppendRetries == 0){
                    logger.debug("[initiateHeartbeatRPC] Successfully got the logAppendEntries");
                    this.state.setLastApplied(this.state.getLastLogIndex());
                    // apply snapshot
                    this.state.getEntries().addAll(this.state.getSnapshot());
                    this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                    rejectionRetries = 0;
                    logAppendRetries = 0;
                    this.state.getSnapshot().clear();
                }
                if(logAppendRetries == MAX_REQUEST_RETRY) {
                    if(rejectionRetries == MAX_REQUEST_RETRY) {
                        logger.info("[initiateHeartbeatRPC] Max rejections seen from the leader! Stepping down!");
                        logAppendRetries = 0;
                        rejectionRetries = 0;
                        this.state.setNodeType(Role.FOLLOWER);
                        return;
                    }
                    rejectionRetries++;
                    logger.info("[initiateHeartbeatRPC] Log append retries max limit reached!!");
                    logger.debug("Before :: LogEntry Size->" + this.state.getEntries().size() + " Snapshot Size->" + this.state.getSnapshot().size());
                    if(this.state.getLastApplied() != -1) {
                        this.state.getEntries().subList((int) this.state.getLastApplied(), (int) this.state.getLastLogIndex() + 1).clear();
                        this.state.getEntries().addAll(this.state.getSnapshot());
                        this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                        this.state.setLastApplied(this.state.getLastLogIndex());
                        logger.debug("After :: " + this.state.getEntries().size());
                        logAppendRetries = 0;
                        this.state.getSnapshot().clear();
                        logger.debug("After :: LogEntry Size->" + this.state.getEntries().size() + " Snapshot Size->" + this.state.getSnapshot().size());
                    }
                }
            } else {
                this.state.getEntries().addAll(this.state.getSnapshot());
                this.state.setLastLogIndex(this.state.getEntries().size() - 1);
                this.state.getSnapshot().clear();
            }
            logger.debug("[initiateHeartbeatRPC] Snapshot :: "+ this.state.getSnapshot());
            logger.debug("[initiateHeartbeatRPC] Entries :: "+ this.state.getEntries());
            logger.debug("[initiateHeartbeatRPC] Current Node is a leader! Reseting the heartbeat timeout to :: "+System.currentTimeMillis());
            this.state.setHeartbeatTrackerTime(System.currentTimeMillis());

            this.state.getEntries().stream().forEach(le ->
                    logger.debug(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue()));
            // @CHECK : UNCOMMENT Below lines to simulate delay in leader during heartbeat
//            if(this.state.getEntries().size() == 5) {
//                logger.debug("[initiateHeartbeatRPC] Simulating leader network partition");
//                Thread.sleep(30 * 1000);
//            }
            cluster.stream().forEach(serv -> {
                if (serv.getServerId() != Integer.parseInt(this.state.getNodeId())) {
                    this.heartBeatExecutor.submit(() -> sendAppendEntries(serv));
                }
            });
        } catch (Exception e) {
            logger.error("[initiateHeartbeatRPC] excp :: "+ e);
        }

    }

    public void initiateLBProcessor() {
        logger.debug("[initiateLBProcessor] Log Entries has " + this.state.getLoadBalancerEntries().size());
        if (!this.state.getNodeType().equals(Role.LEADER)) {
            logger.debug("[initiateLBProcessor] Not a leader! So not participating in Load Balancer Processing!");
            return;
        }
        logger.debug("[initiateLBProcessor] getClusterDetails :: " + this.subClusterList);
        logger.debug("[initiateLBProcessor] getLoadBalancerEntries :: "+this.state.getLoadBalancerEntries().size());
        logger.debug("[initiateLBProcessor] getLoadBalancerSnapshot :: "+this.state.getLoadBalancerSnapshot().size());
        logger.debug("[initiateLBProcessor] getLastLoadBalancerCommitIndex :: "+this.state.getLastLoadBalancerCommitIndex());
        logger.debug("[initiateLBProcessor] getLastLoadBalancerProcessed :: "+this.state.getLastLoadBalancerProcessed());
        logger.debug("[initiateLBProcessor] getLastLoadBalancerLogIndex :: "+this.state.getLastLoadBalancerLogIndex());
        if (this.subClusterList.size() != this.state.getLoadBalancerEntries().size()
                || this.subClusterList.size() != this.state.getLoadBalancerSnapshot().size()
                || this.subClusterList.size() != this.state.getLastLoadBalancerCommitIndex().size()
                || this.subClusterList.size() != this.state.getLastLoadBalancerProcessed().size()
                || this.subClusterList.size() != this.state.getLastLoadBalancerLogIndex().size()) {
            logger.info("Entries and cluster sizes dont match! Possible scnenario of scaling being happening!?");
            return;
        }

        //@TODO :: Make it async to go with all clusters in parallel
//        this.subClusterList.get(clusterIndex).stream().forEach(serv -> {
//                // @TODO :: new executor here!!
//                // @TODO :: send only for cluster leader
//                this.heartBeatExecutor.submit(() -> sendLoadBalancerEntries(serv, clusterIndex));
//            });
        for(int i = 0; i < this.subClusterList.size(); ++i){
            initiateLBProcessorWrapper(i);
        }
    }
    private void initiateLBProcessorWrapper(int clusterIndex) {

        try {
            logger.debug("[initiateLBProcessorWrapper] Last Processed :" + this.state.getLastLoadBalancerProcessed().get(clusterIndex) +
                    " || GetLoadBalancerLastIndex : "+ this.state.getLastLoadBalancerLogIndex().get(clusterIndex) + " || " +this.state.getLoadBalancerProcessStatus().get(clusterIndex));

            // if getLastLogIndex is 0 then its a initial case where currnet node is starting fresh
            // && validating if all the elements within process list (elements between lastProcess - lastLBLogIndex
            if(this.state.getLoadBalancerSnapshot().get(clusterIndex).size() == 0) {
                logger.debug("Nothing to process for this cluster :: "+ clusterIndex);
                return;
            }
            // @TODO :: ideally the diff entires should be cleared if non majority
            if (this.state.getLastLoadBalancerLogIndex().get(clusterIndex) > 0
                    && this.state.getLastLoadBalancerProcessed().get(clusterIndex) < this.state.getLastLoadBalancerLogIndex().get(clusterIndex)
                    && Collections.frequency(this.state.getLoadBalancerProcessStatus().get(clusterIndex), true)
                    != this.state.getLoadBalancerProcessStatus().get(clusterIndex).size()) {
                logger.info("Rejecting entries due to non majority!");
                lbRejectionRetries++;
                if(lbRejectionRetries > MAX_REQUEST_RETRY) {
                    this.state.getLoadBalancerEntries().get(clusterIndex).subList(
                            Math.toIntExact(this.state.getLastLoadBalancerProcessed().get(clusterIndex)),
                            Math.toIntExact(this.state.getLastLoadBalancerLogIndex().get(clusterIndex))).clear();
                    this.state.getLastLoadBalancerProcessed().set(clusterIndex,  this.state.getLastLoadBalancerProcessed().get(clusterIndex) - 1);
                    this.state.getLastLoadBalancerLogIndex().set(clusterIndex, this.state.getLastLoadBalancerProcessed().get(clusterIndex));
                }
                return;
            } else {
                this.state.getLastLoadBalancerProcessed().set(clusterIndex, this.state.getLastLoadBalancerLogIndex().get(clusterIndex));
                this.state.getLoadBalancerEntries().get(clusterIndex).addAll(this.state.getLoadBalancerSnapshot().get(clusterIndex));
                this.state.getLoadBalancerSnapshot().get(clusterIndex).clear();
                this.state.getLoadBalancerProcessStatus().get(clusterIndex).clear();
                if(this.state.getLoadBalancerEntries().get(clusterIndex).size() > 0) {
                    this.state.getLastLoadBalancerLogIndex().set(clusterIndex, (long) (this.state.getLoadBalancerEntries().get(clusterIndex).size()));
                }
                lbRejectionRetries = 0;
            }

            this.state.getLoadBalancerEntries().get(clusterIndex).stream().forEach(le -> {
                    logger.debug(le.getTerm() + " :: " + le.getCommand().getKey() +" -> "+le.getCommand().getValue());
            });
            for (long i = this.state.getLastLoadBalancerProcessed().get(clusterIndex);
                 i < this.state.getLastLoadBalancerLogIndex().get(clusterIndex); ++i) {
                this.state.getLoadBalancerProcessStatus().get(clusterIndex).add(false);
            }
            logger.debug("[initiateLBProcessorWrapper] Snapshot for cluster "+clusterIndex+" :: "+ this.state.getLoadBalancerSnapshot().get(clusterIndex));
            logger.debug("[initiateLBProcessorWrapper] Entries for cluster "+clusterIndex+" :: "+ this.state.getLoadBalancerEntries().get(clusterIndex).size());
            logger.debug("[initiateLBProcessorWrapper] Process Status for cluster "+clusterIndex+" :: "+ this.state.getLoadBalancerProcessStatus().get(clusterIndex));

            sendLoadBalancerEntries(this.subClusterList.get(clusterIndex), clusterIndex);
        } catch (Exception e) {
            logger.error("[initiateLBProcessorWrapper] excp :: "+ e);
            e.printStackTrace();
        }
    }

    private void sendLoadBalancerEntries(Raft.ServerConnect cluster, int clusterIndex) {

        logger.debug("[sendLoadBalancerEntries] : Sending request to " + cluster + " at "+System.currentTimeMillis());
        logger.debug("[sendLoadBalancerEntries] : Last Processed :: "+this.state.getLastLoadBalancerProcessed().get(clusterIndex) +
                " :: Last Log Index :" +this.state.getLastLoadBalancerLogIndex().get(clusterIndex));
        List<Raft.LogEntry> entryToSend = new ArrayList<>();
        Loadbalancer.LoadBalancerRequest.Builder requestBuilder = Loadbalancer.LoadBalancerRequest.newBuilder();
        try {
            if (this.state.getNodeType() != Role.LEADER) {
                logger.debug("[sendLoadBalancerEntries] : Current node is not leader so cant send load balancer entries");
                return;
            }
            if(this.state.getLastLoadBalancerProcessed().get(clusterIndex) == -1) {
                logger.info("[sendLoadBalancerEntries] getLastLoadBalancerProcessed for cluster "+clusterIndex+" is -1");
                return;
            }
            for (long i = this.state.getLastLoadBalancerProcessed().get(clusterIndex);
                 i < this.state.getLastLoadBalancerLogIndex().get(clusterIndex); ++i) {
                if(this.state.getLoadBalancerEntries().get(clusterIndex).size() < i) {
                    logger.info("[sendLoadBalancerEntries] CLuster Entries for "+clusterIndex+" is small than index :: " +i);
                    continue;
                }
                entryToSend.add(this.state.getLoadBalancerEntries().get(clusterIndex).get((int) i));
            }
            requestBuilder.addAllEntries(entryToSend);
            logger.debug("[sendLoadBalancerEntries] Final Request :: "+ requestBuilder.toString());
        } catch (Exception e) {
            logger.debug("[sendLoadBalancerEntries] after response ex : " + e);
        }

        logger.debug("[sendLoadBalancerEntries] : before call : " );
        Raft.Endpoint endpoint = cluster.getEndpoint();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();

        lock.lock();
        try {
            LoadBalancerRequestServiceGrpc.LoadBalancerRequestServiceBlockingStub loadBalancerRequestServiceStub = LoadBalancerRequestServiceGrpc.newBlockingStub(channel);
            Loadbalancer.LoadBalancerResponse response = loadBalancerRequestServiceStub.sendEntries(requestBuilder.build());
            int ind = 0;
            for (long i = this.state.getLastLoadBalancerProcessed().get(clusterIndex);
                 i < this.state.getLastLoadBalancerLogIndex().get(clusterIndex); ++i) {
                logger.debug("[sendLoadBalancerEntries] Response :: "+response.getSuccess(ind));
                this.state.getLoadBalancerProcessStatus().get(clusterIndex).set(Math.toIntExact(i - this.state.getLastLoadBalancerProcessed().get(clusterIndex)), response.getSuccess(ind));

            }
            logger.debug("[sendLoadBalancerEntries] Final status for cluster :: "+this.state.getLoadBalancerProcessStatus().get(clusterIndex));
        } catch (Exception e) {
            logger.debug("[sendLoadBalancerEntries] after response ex : " + e);
            e.printStackTrace();
            for (long i = this.state.getLastLoadBalancerProcessed().get(clusterIndex);
                 i < this.state.getLastLoadBalancerLogIndex().get(clusterIndex); ++i) {
                this.state.getLoadBalancerProcessStatus().get(clusterIndex).set(Math.toIntExact(i - this.state.getLastLoadBalancerProcessed().get(clusterIndex)), false);
            }
        } finally {
            lock.unlock();
            channel.shutdown();
        }
    }
    private void sendAppendEntries(Raft.ServerConnect server) {
        logger.debug("[sendAppendEntries] : Sending request to " + server.getServerId() + " at "+System.currentTimeMillis());
        List<Raft.LogEntry> entryToSend = new ArrayList<>();

        Raft.AppendEntriesRequest.Builder requestBuilder = Raft.AppendEntriesRequest.newBuilder();
        try {
            if (this.state.getNodeType() != Role.LEADER) {
                logger.debug("[sendAppendEntries] : Current node is not leader so cant send heartbeats");
                return;
            }
            int followerIndex = this.state.getNextIndex().get(server.getServerId());
            long peerMatchIndex = this.state.getMatchIndex().get(server.getServerId()); //-1
            long currentIndex = this.state.getCommitIndex();
            long currentTerm = this.state.getCurrentTerm();
            String nodeId = this.state.getNodeId();

            if (followerIndex == 0 || this.state.getEntries().isEmpty()) {
                requestBuilder.setLastAppendedLogIndex(-1);
                requestBuilder.setLastAppendedLogTerm(-1);
            } else {
                requestBuilder.setLastAppendedLogTerm((this.state.getEntries().get((int) peerMatchIndex)).getTerm());
                requestBuilder.setLastAppendedLogIndex(peerMatchIndex);
            }

            requestBuilder.setTerm(currentTerm); // Is this correct ?
            requestBuilder.setLeaderId(nodeId);

            requestBuilder.setCommitIndex(this.state.getCommitIndex()); // Last Commit Index
            List<Raft.LogEntry> entries = this.state.getEntries();
//            logger.debug("[sendAppendEntries] peer match :: " + peerMatchIndex + " : "+this.state.getLastLogIndex());
            //ConvertToInt
            for (long i = peerMatchIndex + 1; i <= this.state.getLastLogIndex(); i++) {
                if(entries.size() <= i) {
                    continue;
                }
                entryToSend.add(entries.get((int) i));
            }
            requestBuilder.addAllEntries(entryToSend);
            requestBuilder.setLastAppliedIndex(this.state.getLastApplied());

            logger.debug("[sendAppendEntries] Final Request :: "+ requestBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("[sendAppendEntries] ex : "+ e);
        }

        logger.debug("[sendAppendEntries] : before call : " + server.getServerId());
        Raft.Endpoint endpoint = server.getEndpoint();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build();

        lock.lock();
        try {
            RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
            Raft.AppendEntriesResponse response = raftServiceBlockingStub.appendEntries(requestBuilder.build());
            boolean success = response.getSuccess();
            if(this.state.getCurrentTerm() < response.getTerm()){
                this.state.setNodeType(Role.FOLLOWER);
                logger.debug("Got rejected, as my term was lower as a leader. This shouldn't be happening");
            }
            if (!success) {
                long term = response.getTerm();

                if (term == -2) {
                    logger.debug("Follower thinks someone else is leader");
                }

                if (term == -3) {
                    this.state.getNextIndex().set(server.getServerId(), (int) response.getLastMatchIndex() + 1);
                    this.state.getMatchIndex().set(server.getServerId(), (int) response.getLastMatchIndex());
                    logger.debug("We have different prev index, this shouldn't happen in this design , but can happen in future");
                }

                if (term == -4) {
                    logger.debug("We have different term is not corrected : self correct");
                }

                if (term == -5) {
                    logger.debug("Follower commit index more then leader!");
                }
            } else {
                logger.debug("[sendAppendEntries] Post Response");
                this.state.getNextIndex().set(server.getServerId(), (int) response.getLastMatchIndex() + 1);
                this.state.getMatchIndex().set(server.getServerId(), (int) response.getLastMatchIndex());
            }
        } catch (Exception e) {
            logger.debug("[sendAppendEntries] after response ex : " + e);
        } finally {
            lock.unlock();
            channel.shutdown();
        }
    }

    /**
     * PreCalls - accessed by client -> job is to read the DB and add to Queue
     * TODO Update requestParam to String and also handle read
     *
     * @param request
     * @return
     */
    public long preCall(Client.Request request) {
        String key = String.valueOf(request.getKey());
        if(subClusterList.isEmpty()){
            log.error("[preCall] wait for your turn you little shit");
            return -5;
        }
        if(db.getCacheEntry().containsKey(key) && request.getCommandType().equals("READ")){
            int clusterId = db.getCacheEntry().get(key).getValue();
            int versionNumber = db.getCacheEntry().get(key).getKey();
            if (subClusterList.size() > clusterId) {

                Raft.LogEntry le = Raft.LogEntry.newBuilder().setCommand(Raft.Command.newBuilder().setCommandType("READ").setKey(String.valueOf(key)).build()).setTerm(this.state.getCurrentTerm()).setIndex("UNIT_TEST").build();
                //MAKE A SERVICE AND CALL
                ManagedChannel channel = ManagedChannelBuilder.forAddress(this.subClusterList.get(clusterId).getEndpoint().getHost(), this.subClusterList.get(clusterId).getEndpoint().getPort()).usePlaintext().build();
                LoadBalancerRequestServiceGrpc.LoadBalancerRequestServiceBlockingStub loadBalancerRequestServiceStub = LoadBalancerRequestServiceGrpc.newBlockingStub(channel);
                Loadbalancer.DataReadRequestObject loadBalancerRequest = Loadbalancer.DataReadRequestObject.newBuilder().setKey(key).setVersion(versionNumber).setTimestamp(String.valueOf(System.currentTimeMillis())).build();
                //Loadbalancer.LoadBalancerResponse response = loadBalancerRequestServiceStub.getEntries();
                //Configuration.ScaleResponse scaleResponse = autoScaleBlockingStub.requestUpScale(scaleRequest);

                this.state.getLoadBalancerSnapshot().get(clusterId).add(le);
                return 1;

            } else {
                return -1; // Error code for cluster Not available;
            }
        }

        if(db.getCacheEntry().containsKey(key) && request.getCommandType().equals("WRITE")){
            logger.info("[UNITTEST-populateSubclusters] From Cache : ");
            String value = String.valueOf(request.getValue());
            Pair<Integer, Integer> integerIntegerPair = db.getCacheEntry().get(key);
            Pair<Integer,Integer> pair = new Pair<>(integerIntegerPair.getKey()+1, integerIntegerPair.getValue());

            int clusterId = pair.getValue();
            if (subClusterList.size() > clusterId) {
                int versionNumber = pair.getKey();
                int clusterToBeSent = this.getState().getUtilizationMap().isEmpty() ? 0 : this.getState().getUtilizationMap().firstKey();
                Raft.LogEntry le = Raft.LogEntry.newBuilder().setCommand(
                                                Raft.Command.newBuilder().setCommandType("WRITE")
                                                                        .setValue(String.valueOf(value))
                                                                        .setKey(String.valueOf(key))
                                                                        .setVersion(versionNumber).build())
                                                .setTerm(this.state.getCurrentTerm())
                                                .setClusterId(clusterToBeSent)
                                                .setIndex("UNIT_TEST").build();
                logger.info("[UNITTEST-populateSubclusters] Adding via Cache : "+ le);
                this.state.getLoadBalancerSnapshot().get(clusterToBeSent).add(le);
                return 1;

            } else {
                return -1; // Error code for cluster Not available;
            }
        }
        ReadLBObject readLBObject = db.read(String.valueOf(key));
        int retVal = readLBObject.getReturnVal();

        if (retVal < 0 || retVal == 2 && request.getCommandType().equals("READ")) {
            logger.error("[LoadBalancerServer] something went wrong, Please investigate: ");
            return -2; // Error code DB READ failure
        }

        if (request.getCommandType().equals("READ")) {
            int clusterId = readLBObject.getClusterId();
            if (subClusterList.size() > clusterId) {
                Raft.LogEntry le = Raft.LogEntry.newBuilder().setCommand(Raft.Command.newBuilder().setCommandType("READ").setKey(String.valueOf(key)).build()).setTerm(this.state.getCurrentTerm()).setIndex("UNIT_TEST").build();
//                this.state.getLoadBalancerSnapshot().get(clusterId).add(le);
                // @TODO :: Add service call
                return 1;

            } else {
                return -1; // Error code for cluster Not available;
            }
        } else if (request.getCommandType().equals("WRITE")) {
            int versionNumber = readLBObject.getVersionNumber();
            long k = request.getKey();
            long v = request.getValue();
            
            //TODO
            int clusterToBeSent = this.getState().getUtilizationMap().isEmpty() ? 0 : this.getState().getUtilizationMap().firstKey();
            //AddToTheQueue
            Raft.LogEntry le = Raft.LogEntry.newBuilder().setCommand(
                            Raft.Command.newBuilder().setCommandType("WRITE")
                                    .setValue(String.valueOf(v))
                                    .setKey(String.valueOf(k))
                                    .setVersion(versionNumber + 1).build())
                    .setTerm(this.state.getCurrentTerm())
                    .setClusterId(clusterToBeSent)
                    .setIndex("UNIT_TEST").build();
            logger.info("[UNITTEST-populateSubclusters] Adding le : "+ le);
            //TODO be more stirngent
            db.getCacheEntry().put(key, new Pair<>(versionNumber + 1, clusterToBeSent));
            this.state.getLoadBalancerSnapshot().get(clusterToBeSent).add(le);
            return 1;
        } else if (request.getCommandType().equals("DELETE")) {
            int clusterId = readLBObject.getClusterId();
            //Build request and add to the queue
        } else {
            logger.error("[LoadBalancerServer], Command not supported");
            return -5;
        }
        logger.error("[LoadBalancerServer], shouldn't reach here");

        return -6;
    }

//    public int putValue(long key, long val, String clientKey) {
//        lock.lock();
//        try {
//            int numOfEntries = 1;
//            //TODO should we pull the leader check code there ?
//            //TODO add cmd type from params and make this into a loop
//            Raft.Command command = Raft.Command.newBuilder().setCommandType("Something").setKey(key).setValue(val).build();
//            String requestId = String.valueOf(UUID.randomUUID());
//            Raft.LogEntry logEntry = Raft.LogEntry.newBuilder().setRequestId(requestId)
//                    .setCommand(command)
//                    .setIndex(String.valueOf(this.state.getEntries().size()))
//                    .setTerm(this.state.getCurrentTerm()).build();
//
//            this.persistentStore.put(requestId, new Pair<>(clientKey,null));
//            this.state.getSnapshot().add(logEntry);
//            logger.debug("Created request with id :: "+ requestId);
//            return 0;
//        } finally {
//            lock.unlock();
//        }
//    }

    private void initiateCleanUpVersions(){
        if (this.state.getNodeType() != Role.LEADER) {
            logger.debug("[initiateCleanUpVersions] : Current node is not leader so cant send heartbeats");
            return;
        }
        db.cleanUp();
    }

    private void initiateLiveLinessProbesRPC() {

        try {
            if (this.state.getNodeType() != Role.LEADER) {
                logger.debug("[initiateLiveLinessProbesRPC] : Current node is not leader so cant send heartbeats");
                return;
            }
            if(subClusterList == null || subClusterList.size() == 0){
                logger.debug("[initiateLiveLinessProbesRPC] Initializing subclusters!");
                scaleUp();
                return;
            }
            AtomicInteger count = new AtomicInteger();
            logger.debug("[initiateLiveLinessProbesRPC] subClusterList usage :: " +  subClusterList.size() );
            List<Callable<Pair<Integer,Double>>> callableTasks= subClusterList.stream().map(sc -> {
                logger.debug("Inside ::" + sc + " :: "+count.get());
                return loadBalancerLiveLinessService.wrapperCallBack(sc, count.getAndIncrement());
            }).collect(Collectors.toList());
            List<Future<Pair<Integer,Double>>> collect = liveLinessExecutor.invokeAll(callableTasks);
            logger.debug("[initiateLiveLinessProbesRPC] collect :" + collect + " : " + subClusterList);
            double sum = 0;
            for (int i = 0; i < collect.size(); i++) {
                try {
                    Pair<Integer,Double> temp = collect.get(i).get();
                    double utilization = temp.getValue();
                    int clusterId = temp.getKey();
                    // @TODO handle return checks (i.e we are returning negative values and filer it)
                    logger.debug("[initiateLiveLinessProbesRPC] result :: "+temp);
                    this.getState().getUtilizationMap().put(clusterId, utilization);
                    sum += utilization;
                } catch (InterruptedException e) {
                    logger.error("[initiateLiveLinessProbesRPC] Exception found in  :: ", e);
                } catch (ExecutionException e) {
                    logger.error("[initiateLiveLinessProbesRPC] Exception found in  :: ", e);
                }
            }
            logger.info("[initiateLiveLinessProbesRPC] CLuster usage :: " +  sum / subClusterList.size() );
            if (sum / subClusterList.size() > 80) {
                scaleUp();
            }
        }
        catch(Exception e){
            logger.error("[initiateLiveLinessProbesRPC] Found an exception, please check", e);
        }
    }

    private void scaleUp() {
        Configuration.ScaleRequest scaleRequest = Configuration.ScaleRequest.newBuilder().setAbsoluteMajority(2).setClusterSize(1).build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(autoScalerHostName, autoScalerPortNumber).usePlaintext().build();
        AutoScaleGrpc.AutoScaleBlockingStub autoScaleBlockingStub = AutoScaleGrpc.newBlockingStub(channel);
        Configuration.ScaleResponse scaleResponse = autoScaleBlockingStub.requestUpScale(scaleRequest);
//                Cluster.ClusterConnect clusterConnect = scaleResponse.getClusterDetails();
        //Cluster.ClusterConnect clusterConnect = scaleResponse.getClusterDetails();
        boolean isCreated = scaleResponse.getIsCreated();
        boolean inProgress = scaleResponse.getInProgress();
        List<Configuration.ServerDetails> subClustersList = scaleResponse.getSubClustersList();
        logger.debug("[scaleUp] " + subClusterList + " : " + isCreated + " : " + inProgress) ;
        if (isCreated && inProgress) {
            //Assign Key Range to this or whatever
            lock.lock();
            try {
                // @TODO :: Check if there are any other places for these init
                for (int i = 0; i < subClustersList.size(); i++) {
                    int x = subClusterList.size();
                    this.state.getLoadBalancerEntries().add(new ArrayList<>());
                    this.state.getLoadBalancerSnapshot().add(new ArrayList<>());
                    this.state.getLoadBalancerProcessStatus().add(new ArrayList<>());
                    this.state.getLastLoadBalancerCommitIndex().add((long) -1);
                    this.state.getLastLoadBalancerProcessed().add((long) -1);
                    this.state.getLastLoadBalancerLogIndex().add(0L);
                    this.state.getUtilizationMap().put(x,0.0);
                    subClusterList.add(subClustersList.get(i).getServerConnectsList().get(0));
                }
            } finally {
                lock.unlock();
            }
        }
    }


}
