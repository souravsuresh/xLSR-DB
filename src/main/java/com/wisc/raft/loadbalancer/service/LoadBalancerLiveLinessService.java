package com.wisc.raft.loadbalancer.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.*;
import com.wisc.raft.proto.Cluster;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.service.RaftConsensusService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.wisc.raft.proto.UtilizationServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoadBalancerLiveLinessService {

    //Also need to threading.

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerLiveLinessService.class);

    ConcurrentHashMap<String,Integer> checkStatus; // For now putting this here, need to be accessed by other service which needs to rest this to zero

    List<Cluster.ClusterConnect> subClusterList;



    int limit;

    LoadBalancerLiveLinessService(List<Cluster.ClusterConnect> subClusterList, int limit){
        this.subClusterList = subClusterList;
        checkStatus = new ConcurrentHashMap<>();
        subClusterList.stream().forEach(sc -> {
            checkStatus.put(String.valueOf(sc.getClusterLeaderId()), 0);
        });
        this.limit = limit;
    }




    public void checkLiveliness(Cluster.ClusterConnect  clusterConnect){
        Raft.UtilizationRequest.Builder utilizationRequestBuilder = Raft.UtilizationRequest.newBuilder();
        Raft.UtilizationRequest utilizationRequest = utilizationRequestBuilder.setLeaderId(String.valueOf(clusterConnect.getClusterLeaderId())).build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress(clusterConnect.getClusterEndpoint().getHost(), clusterConnect.getClusterEndpoint().getPort()).usePlaintext().build();
        try {
            UtilizationServiceGrpc.UtilizationServiceBlockingStub utilizationServiceBlockingStub = UtilizationServiceGrpc.newBlockingStub(channel);
            Raft.UtilizationResponse respone = utilizationServiceBlockingStub.getUtilization(utilizationRequest);
            int retValue = respone.getReturnVal();
            if(retValue == 1){
                //Reset
                checkStatus.put(String.valueOf(clusterConnect.getClusterLeaderId()),  0);
            }
            else if(retValue == -1){
                logger.error("[LoadBalancerLiveLinessService] exception occurred during calculation");
            }
            else if(retValue == -2){
                logger.error("[LoadBalancerLiveLinessService] Not a leader, invoke find leader logic");
                //Option he sends leader Info ?
                //What happens if he gives the leader info and it is 3 attempt ,one last attempt ?
                if(checkStatus.get(String.valueOf(clusterConnect.getClusterLeaderId())) > 3){
                    //Find leader logic.
                    //Hard attempt
                }
                else{
                    //softAttempt
                    Raft.Endpoint newLeaderEndpoint = respone.getEndpoint();
                    checkStatus.put(String.valueOf(clusterConnect.getClusterLeaderId()),  checkStatus.get(clusterConnect.getClusterLeaderId()) + 1);

                }

            }
            else if(retValue == -3){
                logger.error("[LoadBalancerLiveLinessService] Threshold breached, update count");
                checkStatus.put(String.valueOf(clusterConnect.getClusterLeaderId()),  checkStatus.get(clusterConnect.getClusterLeaderId()) + 1);
                if(checkStatus.get(String.valueOf(clusterConnect.getClusterLeaderId())) >= 3){
                    logger.error("Split please");
                    //invoke split logix
                }
            }

        }
        catch(Exception e){
            logger.error("[LoadBalancerLiveLinessService] exception in grpc request :" + e.getMessage());
        }
        finally {
            channel.shutdown();
        }

    }
}
