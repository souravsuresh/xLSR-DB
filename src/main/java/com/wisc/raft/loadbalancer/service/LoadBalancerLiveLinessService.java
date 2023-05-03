package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class LoadBalancerLiveLinessService {

    //Also need to threading.

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerLiveLinessService.class);

    ConcurrentHashMap<Integer,Integer> checkStatus; // For now putting this here, need to be accessed by other service which needs to rest this to zero

    int retryLimit;

    //Assuming this comes from somewhere
    LoadBalancerLiveLinessService(ConcurrentHashMap<Integer,Integer> checkStatus, int limit){
        this.checkStatus = checkStatus;
        this.retryLimit = limit;
    }




    public void checkLiveliness(Cluster.ClusterConnect  clusterConnect){
        Raft.UtilizationRequest.Builder utilizationRequestBuilder = Raft.UtilizationRequest.newBuilder();
        Raft.UtilizationRequest utilizationRequest = utilizationRequestBuilder.setLeaderId(String.valueOf(clusterConnect.getClusterLeaderId())).build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(clusterConnect.getClusterEndpoint().getHost(), clusterConnect.getClusterEndpoint().getPort()).usePlaintext().build();
        try {
            UtilizationServiceGrpc.UtilizationServiceBlockingStub utilizationServiceBlockingStub = UtilizationServiceGrpc.newBlockingStub(channel);
            Raft.UtilizationResponse response = utilizationServiceBlockingStub.getUtilization(utilizationRequest);
            int retValue = response.getReturnVal();
            if(retValue == 1){
                //Reset
                checkStatus.put(clusterConnect.getClusterLeaderId(),  0);
            }
            else if(retValue == -1){
                logger.error("[LoadBalancerLiveLinessService] exception occurred during calculation or other generic crap");
            }
            else if(retValue == -2){
                logger.error("[LoadBalancerLiveLinessService] Something went wrong not sure is this required");
            }
            else if(retValue == -3){
                logger.error("[LoadBalancerLiveLinessService] Threshold breached, update count");
                checkStatus.put(clusterConnect.getClusterId(),  checkStatus.get(clusterConnect.getClusterLeaderId()) + 1);
                if(checkStatus.get(String.valueOf(clusterConnect.getClusterId())) >= 3){
                    logger.error("Split please");
                    Configuration.ScaleRequest scaleRequest = Configuration.ScaleRequest.newBuilder().setAbsoluteMajority(2).setClusterSize(3).build();


                    Optional<Cluster.ClusterConnect> optClusterConnect = callAutoScaler(scaleRequest);
                    //Use this to do something still figuring out
                }
            }
            else if(retValue == -4){
                logger.error("[LoadBalancerLiveLinessService] Hit A candidate");
                checkStatus.put(clusterConnect.getClusterId(), checkStatus.get(clusterConnect.getClusterId()) + 1);
            }
            else if(retValue == -2){
                logger.error("[LoadBalancerLiveLinessService] Hit A Follower");
                checkStatus.put(clusterConnect.getClusterId(), checkStatus.get(clusterConnect.getClusterId()) + 1);

            }

        }
        catch(Exception e){
            logger.error("[LoadBalancerLiveLinessService] exception in grpc request :" + e.getMessage());
        }
        finally {
            channel.shutdown();
        }

    }

    //TODO:  Probably not a good idea, doing this like this as thread would get killed, need to think of cleaner approach
    private Optional<Cluster.ClusterConnect> callAutoScaler(Configuration.ScaleRequest scaleRequest) {
        //TODO get this from server;
        logger.info("[callAutoScaler] Calling AutoScaler");
        ManagedChannel autoScaleChannel = ManagedChannelBuilder.forAddress("localHost", 10000).usePlaintext().build();
        try{
            AutoScaleGrpc.AutoScaleBlockingStub autoScaleBlockingStub = AutoScaleGrpc.newBlockingStub(autoScaleChannel);
            Configuration.ScaleResponse scaleResponse = autoScaleBlockingStub.requestUpScale(scaleRequest);
            Cluster.ClusterConnect clusterConnect = scaleResponse.getClusterDetails();
            boolean isCreated = scaleResponse.getIsCreated();
            boolean inProgress = scaleResponse.getInProgress();
            if(isCreated && inProgress){
                //Assign Key Range to this or whatever
            }
            else{
                logger.error("[callAutoScaler] Error from autoscaler");
            }
            return Optional.of(clusterConnect);

        }
        finally {
            logger.error("[callAutoScaler] Error from autoscaler");
            autoScaleChannel.shutdown();
        }
    }
}
