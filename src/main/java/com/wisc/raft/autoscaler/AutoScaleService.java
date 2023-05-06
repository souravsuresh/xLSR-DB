package com.wisc.raft.autoscaler;

import com.wisc.raft.loadbalancer.server.LoadBalancerServer;
import com.wisc.raft.loadbalancer.service.SubClusterLeaderService;
import com.wisc.raft.proto.AutoScaleGrpc;
import com.wisc.raft.proto.Cluster;
import com.wisc.raft.proto.Configuration;
import com.wisc.raft.proto.Raft;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AutoScaleService extends AutoScaleGrpc.AutoScaleImplBase {

    private static final Logger logger = LoggerFactory.getLogger(AutoScaleService.class);
    Scanner scanner ;

    AutoScaleService(){
        this.scanner = new Scanner(System.in);
    }

    //TODO Add the leader details into this
    @Override
    public void requestUpScale(Configuration.ScaleRequest request, StreamObserver<Configuration.ScaleResponse> response) {
        logger.debug("[requestUpScale] Inside Request UP SCALE");

        Configuration.ScaleResponse.Builder scaleResponseBuilder = Configuration.ScaleResponse.newBuilder();
        try{
            int count = request.getClusterSize();
            for(int i=0;i<count;i++){
                logger.info("Please enter 1 cluster details in a single line, With leader details first");
                String clusterString = scanner.nextLine();
                String[] s = clusterString.split("_");
                Configuration.ServerDetails.Builder serverDetailsBuilder = Configuration.ServerDetails.newBuilder();

                for(int j=0;j<s.length-2;j=j+3){
                    //seperated id_hostname_port
                    Raft.Endpoint endpoint = Raft.Endpoint.newBuilder().setHost(s[j+1]).setPort(Integer.parseInt(s[j+2])).build();
                    Raft.ServerConnect serverConnect = Raft.ServerConnect.newBuilder().setEndpoint(endpoint).setServerId(Integer.parseInt(s[j])).build();
                    serverDetailsBuilder.addServerConnects(serverConnect);

                }
                scaleResponseBuilder.addSubClusters(serverDetailsBuilder.build());
            }

        }
        catch (Exception e) {
            logger.error("[requestUpScale] Something went wrong while executing");
            throw new RuntimeException(e);
        }
        //Invoke pooling to figure out leader , similar to the client code
        //Should we have a way to poll ; doubt this is required.
        //Either return something or make changes in a common object : TODO , Preferebly return then you could dictate what needs to happen here for now returning true;
        scaleResponseBuilder.setInProgress(true).setIsCreated(true).build();
        response.onNext(scaleResponseBuilder.build());
        response.onCompleted();

    }

    @Override
    public void requestDownScale(Configuration.ScaleDownRequest request, StreamObserver<Configuration.ScaleResponse> response) {
//        logger.debug("[requestUpScale] Inside Request Down SCALE");
//        Configuration.ScaleResponse.Builder scaleResponseBuilder = Configuration.ScaleResponse.newBuilder();
//        int count = request.getClustersCount();
//        List<Cluster.ClusterConnect> clusterEndpointList = new ArrayList<>();
//
//        while(count != 0){
//            clusterEndpointList.add(request.getClusters((count - 1)));
//            count--;
//        }
//        //TODO Define a scheme to merge Cluster.
//        response.onNext(scaleResponseBuilder.build());
//        response.onCompleted();
    }
}