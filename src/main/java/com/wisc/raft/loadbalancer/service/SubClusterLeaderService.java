package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.proto.Cluster;
import com.wisc.raft.proto.LeaderServiceGrpc;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.UtilizationServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class SubClusterLeaderService {

    private static final Logger logger = LoggerFactory.getLogger(SubClusterLeaderService.class);

    public ConcurrentHashMap<Integer, Cluster.ClusterConnect> getSubClusterMap() {
        return subClusterMap;
    }

    //Assuming initially this fill be filled by a random node details
    ConcurrentHashMap<Integer, Cluster.ClusterConnect> subClusterMap;
    ConcurrentHashMap<Integer, Integer> retryCount;


    public void setSubClusterMap(ConcurrentHashMap<Integer, Cluster.ClusterConnect> subClusterMap) {
        this.subClusterMap = subClusterMap;
    }


    //TODO Add threading later
    public void findLeaders(){
        this.subClusterMap.entrySet().stream().filter(entry -> retryCount.get(entry.getValue().getClusterId()) < 3).forEach(entry -> {
            Cluster.ClusterConnect endpoint = entry.getValue();
            makeCall(endpoint);
        });

    }
    
    private void makeCall(Cluster.ClusterConnect clusterConnect){
        ManagedChannel channel = ManagedChannelBuilder.forAddress(clusterConnect.getClusterEndpoint().getHost(), clusterConnect.getClusterEndpoint().getPort()).usePlaintext().build();
        Cluster.RequestLeaderRPC requestLeader = Cluster.RequestLeaderRPC.newBuilder().setLeaderId(String.valueOf(clusterConnect.getClusterLeaderId())).build();
        try {
            LeaderServiceGrpc.LeaderServiceBlockingStub leaderServiceBlockingStub = LeaderServiceGrpc.newBlockingStub(channel);
            Cluster.ResponseLeaderRPC leaderResponse = leaderServiceBlockingStub.getLeaderConnect(requestLeader);
            int retVal = leaderResponse.getRetValue();
            if(retVal == -1){
                //Retry logic
                logger.error("[SubClusterLeaderService] Hit a candidate, need to retry");
                retryCount.put(clusterConnect.getClusterId(), retryCount.get(clusterConnect.getClusterId() + 1));
                if(retryCount.get(clusterConnect.getClusterId()) < 3){
                    makeCall(clusterConnect);
                }
                else{
                    logger.error("[SubClusterLeaderService] Hit a candidate, no more retries");
                    return;
                }
            }
            else if(retVal == -2){
                //How to handle this
                logger.error("[SubClusterLeaderService] Hit a follower, shall we assume");
            }
            else if(retVal == -3){
                logger.error("[SubClusterLeaderService] This should never happen please check");
            }
            else if(retVal == 0){
                logger.info("[SubClusterLeaderService] I think I found the leader");
                subClusterMap.put(clusterConnect.getClusterId(), clusterConnect);
            }
        }
        catch(Exception e){
            logger.error("[SubClusterLeaderService] found an exception :", e.getMessage());
        }

        finally {
            channel.shutdownNow();
        }
    }

}
