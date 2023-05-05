package com.wisc.raft.autoscaler;

import com.wisc.raft.loadbalancer.service.SubClusterLeaderService;
import com.wisc.raft.proto.AutoScaleGrpc;
import com.wisc.raft.proto.Cluster;
import com.wisc.raft.proto.Configuration;
import com.wisc.raft.proto.LeaderServiceGrpc;
import com.wisc.raft.subcluster.service.LoadBalancerClusterConnectionService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AutoScaleService extends AutoScaleGrpc.AutoScaleImplBase {

    private static final Logger logger = LoggerFactory.getLogger(AutoScaleService.class);

    SubClusterLeaderService subClusterLeaderService;

    //TODO Add the leader details into this
    @Override
    public void requestUpScale(Configuration.ScaleRequest request, StreamObserver<Configuration.ScaleResponse> response) {
        logger.debug("[requestUpScale] Inside Request UP SCALE");

        Configuration.ScaleResponse.Builder scaleResponseBuilder = Configuration.ScaleResponse.newBuilder();
        try {
            //Ideally you pass on cluster Info and leader replace this then, would also require you to poll till you get the leader which should be straightforward
            ProcessBuilder pb = new ProcessBuilder("java", "-classpath", "/Users/varun/Documents/DistFinalProject/xLSR-DB/src/main/java/com/wisc/raft/loadbalancer/service/Temp.java", "Temp");
            Process process = pb.start();
            while (!process.isAlive()) {
                Thread.sleep(100); // wait for 100 milliseconds before checking again
            }

        } catch (IOException e) {
            logger.error("[requestUpScale] Something went wrong while executing");
            scaleResponseBuilder.setInProgress(false).setIsCreated(false).build();
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("[requestUpScale] Something went wrong while executing");
            throw new RuntimeException(e);
        }
        //Invoke pooling to figure out leader , similar to the client code
        //Should we have a way to poll ; doubt this is required.
        Cluster.ClusterConnect clusterConnect = null;
        //Either return something or make changes in a common object : TODO , Preferebly return then you could dictate what needs to happen here for now returning true;
        subClusterLeaderService.makeCall(clusterConnect);
        scaleResponseBuilder.setInProgress(false).setIsCreated(true).build();
        response.onNext(scaleResponseBuilder.build());
        response.onCompleted();

    }

    @Override
    public void requestDownScale(Configuration.ScaleDownRequest request, StreamObserver<Configuration.ScaleResponse> response) {
        logger.debug("[requestUpScale] Inside Request Down SCALE");
        Configuration.ScaleResponse.Builder scaleResponseBuilder = Configuration.ScaleResponse.newBuilder();
        int count = request.getClustersCount();
        List<Cluster.ClusterConnect> clusterEndpointList = new ArrayList<>();

        while(count != 0){
            clusterEndpointList.add(request.getClusters((count - 1)));
            count--;
        }
        //TODO Define a scheme to merge Cluster.
        response.onNext(scaleResponseBuilder.build());
        response.onCompleted();
    }
}