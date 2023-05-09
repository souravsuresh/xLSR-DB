package com.wisc.raft.loadbalancer;

import com.wisc.raft.loadbalancer.service.LoadBalancerLiveLinessService;
import com.wisc.raft.loadbalancer.service.ServerClientConnectionService;
import com.wisc.raft.proto.AutoScaleGrpc;
import com.wisc.raft.proto.Configuration;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.loadbalancer.server.LoadBalancerServer;
import com.wisc.raft.loadbalancer.service.LoadBalancerDatabase;
import com.wisc.raft.loadbalancer.service.RaftConsensusService;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class RaftServer {

    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);
    private static final Random random = new Random();

    /*
        args[0]  = current server id;
        args[1]  = current server port number;
        args[2]..args[n-1]  = all servers in cluster (space seperated id_hostname_port)
    */
    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("[RaftServer] Starting the main server!!");
        String levelDB = "./leveldb_"+random.nextInt(100);
        logger.info("LevelDB folder :: "+levelDB);
        LoadBalancerDatabase database = new LoadBalancerDatabase(levelDB);
        List<Raft.ServerConnect> serverList = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            String[] arg = args[i].split("_");
            if (arg[0] == args[0]) continue;
            int serverId = Integer.parseInt(arg[0]);

            Raft.Endpoint endpoint = Raft.Endpoint.newBuilder().setHost(arg[1]).setPort(Integer.parseInt(arg[2])).build();
            Raft.ServerConnect server = Raft.ServerConnect.newBuilder().setServerId(serverId).setEndpoint(endpoint).build();
            serverList.add(server);
        }
        LoadBalancerServer raftServer = new LoadBalancerServer(args[0], database,  9999, "localhost" );
        raftServer.setCluster(serverList);
        ServerClientConnectionService clientConnectionService = new ServerClientConnectionService(raftServer);
        RaftConsensusService raftConsensusService = new RaftConsensusService(raftServer);
        logger.debug("[Sys Args] "+args.toString());
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[1]))
                                .addService(raftConsensusService)
                                .addService(clientConnectionService).build();
//        Configuration.ScaleRequest scaleRequest = Configuration.ScaleRequest.newBuilder().setAbsoluteMajority(2).setClusterSize(1).build();
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9999).usePlaintext().build();
//        AutoScaleGrpc.AutoScaleBlockingStub autoScaleBlockingStub = AutoScaleGrpc.newBlockingStub(channel);
//        Configuration.ScaleResponse scaleResponse = autoScaleBlockingStub.requestUpScale(scaleRequest);

        //Start the server
        raftServer.init();
        server.start();
        server.awaitTermination();
    }
}