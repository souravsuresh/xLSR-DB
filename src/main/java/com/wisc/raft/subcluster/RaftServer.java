package com.wisc.raft.subcluster;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.server.Server;
import com.wisc.raft.subcluster.service.Database;
import com.wisc.raft.subcluster.service.LoadBalancerService;
import com.wisc.raft.subcluster.service.RaftConsensusService;
import com.wisc.raft.subcluster.storagestate.StorageState;
import com.wisc.raft.subcluster.storagestate.UtilizationService;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.*;

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
        String levelDB = "./leveldb_"+args[1]+"_"+random.nextInt(100)+"_"+args[0];
        logger.info("LevelDB folder :: "+levelDB);
        Database database = new Database(levelDB);
        List<Raft.ServerConnect> serverList = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            String[] arg = args[i].split("_");
            if (arg[0] == args[0]) continue;
            int serverId = Integer.parseInt(arg[0]);

            Raft.Endpoint endpoint = Raft.Endpoint.newBuilder().setHost(arg[1]).setPort(Integer.parseInt(arg[2])).build();
            Raft.ServerConnect server = Raft.ServerConnect.newBuilder().setServerId(serverId).setEndpoint(endpoint).build();
            serverList.add(server);
        }
        Server raftServer = new Server(args[0], database);
        raftServer.setCluster(serverList);
        //ServerClientConnectionService clientConnectionService = new ServerClientConnectionService(raftServer);
        RaftConsensusService raftConsensusService = new RaftConsensusService(raftServer);
        logger.debug("[Sys Args] "+args.toString());

        StorageState storageState = new StorageState(levelDB, 10097360);     // 5MB
        UtilizationService utilizationService = new UtilizationService(raftServer, storageState);
        LoadBalancerService loadBalancerService = new LoadBalancerService(raftServer);
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[1]))
                .addService(raftConsensusService)
                .addService(utilizationService)
                .addService(loadBalancerService).build();

        //Start the server
        raftServer.init();
        server.start();
        server.awaitTermination();
    }
}
