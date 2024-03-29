package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.loadbalancer.server.LoadBalancerServer;
import com.wisc.raft.proto.AutoScaleGrpc;
import com.wisc.raft.proto.Configuration;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.UtilizationServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class LoadBalancerLiveLinessService {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerLiveLinessService.class);

    LoadBalancerServer loadBalancerServer;
    ConcurrentHashMap<Integer,Integer> checkStatus; // For now putting this here, need to be accessed by other service which needs to rest this to zero
    ConcurrentHashMap<Integer,Double> prevUtilization; // For now putting this here, need to be accessed by other service which needs to rest this to zero

    Lock lock;


    int retryLimit;

    public Callable<Pair<Integer, Double>> wrapperCallBack(Raft.ServerConnect  clusterConnect, int clusterId) {
        Callable<Pair<Integer, Double>> callableTask = () -> new Pair<>(clusterId,checkLiveliness(clusterConnect, clusterId));
        return callableTask;
    }

    //Assuming this comes from somewhere
    public LoadBalancerLiveLinessService(ConcurrentHashMap<Integer,Integer> checkStatus, int limit){
        this.checkStatus = checkStatus;
        prevUtilization = new ConcurrentHashMap();
        this.retryLimit = limit;
        this.lock = new ReentrantLock();
    }




    public double checkLiveliness(Raft.ServerConnect  clusterConnect,int clusterId){
        Raft.UtilizationRequest.Builder utilizationRequestBuilder = Raft.UtilizationRequest.newBuilder();
        Raft.UtilizationRequest utilizationRequest = utilizationRequestBuilder.setLeaderId(String.valueOf(0)).build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(clusterConnect.getEndpoint().getHost(), clusterConnect.getEndpoint().getPort()).usePlaintext().build();
        try {
            UtilizationServiceGrpc.UtilizationServiceBlockingStub utilizationServiceBlockingStub = UtilizationServiceGrpc.newBlockingStub(channel);
            Raft.UtilizationResponse response = utilizationServiceBlockingStub.getUtilization(utilizationRequest);
            int retValue = response.getReturnVal();
            if(retValue == 1){
                //Reset

                checkStatus.put(clusterId,  0);
                prevUtilization.put(clusterId, (double) response.getUtilizationPercentage());
                return response.getUtilizationPercentage();
            }
            else if(retValue == -1){
                logger.error("[LoadBalancerLiveLinessService] exception occurred during calculation or other generic crap");
                if(prevUtilization.containsKey(clusterId)){
                    return prevUtilization.get(clusterId);
                }
                else{
                    return 100;
                }
            }
            else if(retValue == -2){
                logger.error("[LoadBalancerLiveLinessService] Something went wrong not sure is this required");
                if(prevUtilization.containsKey(clusterId)){
                    return prevUtilization.get(clusterId);
                }
                else{
                    return 100;
                }

            }
            else if(retValue == -3){
                logger.error("[LoadBalancerLiveLinessService] Threshold breached, update count");
                checkStatus.put(clusterId,  checkStatus.get(clusterId) + 1);
                if(checkStatus.get(String.valueOf(clusterId)) >= 3){
                    return 100;
                    //Use this to do something still figuring out
                }
                else{
                    if(prevUtilization.containsKey(clusterId)){
                        return prevUtilization.get(clusterId);
                    }
                    else{

                        return 100;
                    }

                }
            }
            else if(retValue == -4){
                logger.error("[LoadBalancerLiveLinessService] Hit A candidate");
                checkStatus.put(clusterId, checkStatus.get(clusterId) + 1);
                if(prevUtilization.containsKey(clusterId)){
                    return prevUtilization.get(clusterId);
                }
                else{
                    return 100;
                }
            }
            else if(retValue == -2){
                logger.error("[LoadBalancerLiveLinessService] Hit A Follower");
                checkStatus.put(clusterId, checkStatus.get(clusterId) + 1);
                if(prevUtilization.containsKey(clusterId)){
                    return prevUtilization.get(clusterId);
                }
                else{
                    return 100;
                }
            }

        }
        catch(Exception e){
            logger.error("[LoadBalancerLiveLinessService] exception in grpc request :" + e.getMessage());
        }
        finally {
            channel.shutdown();
        }

        if(prevUtilization.containsKey(clusterId)){
            return prevUtilization.get(clusterId);
        }
        else{
            return 100;
        }

    }

    //TODO:  Probably not a good idea, doing this like this as thread would get killed, need to think of cleaner approach
    private Boolean callAutoScaler(Configuration.ScaleRequest scaleRequest) {
        //TODO get this from server;
        logger.info("[callAutoScaler] Calling AutoScaler");
        ManagedChannel autoScaleChannel = ManagedChannelBuilder.forAddress("localHost", 10000).usePlaintext().build();
        try{
            AutoScaleGrpc.AutoScaleBlockingStub autoScaleBlockingStub = AutoScaleGrpc.newBlockingStub(autoScaleChannel);
            Configuration.ScaleResponse scaleResponse = autoScaleBlockingStub.requestUpScale(scaleRequest);
            //Cluster.ClusterConnect clusterConnect = scaleResponse.getClusterDetails();
            boolean isCreated = scaleResponse.getIsCreated();
            boolean inProgress = scaleResponse.getInProgress();
            List<Configuration.ServerDetails> subClustersList = scaleResponse.getSubClustersList();
            if(isCreated && inProgress){
                lock.lock();
                try {
                    ConcurrentHashMap<Integer, List<Raft.ServerConnect>> clusterDetails = loadBalancerServer.getState().getClusterDetails();
                    List<List<Raft.LogEntry>> loadBalancerEntries = loadBalancerServer.getState().getLoadBalancerEntries();
                    List<List<Raft.LogEntry>> loadBalancerSnapshots = loadBalancerServer.getState().getLoadBalancerSnapshot();
                    List<List<Boolean>> loadBalancerProcessStates = loadBalancerServer.getState().getLoadBalancerProcessStatus();
                    for (int i = 0; i < subClustersList.size(); i++) {
                        int x = clusterDetails.size();
                        clusterDetails.put(x, subClustersList.get(i).getServerConnectsList());
                        loadBalancerEntries.add(new ArrayList<>());
                        loadBalancerSnapshots.add(new ArrayList<>());
                        loadBalancerProcessStates.add(new ArrayList<>());
                    }
                }
                finally {
                    lock.unlock();
                }
                return true;
            }
            else{
                logger.error("[callAutoScaler] Error from autoscaler");
                return false;
            }

        }
        finally {
            logger.error("[callAutoScaler] Error from autoscaler");
            autoScaleChannel.shutdown();
        }
    }
}
