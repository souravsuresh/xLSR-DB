package com.wisc.raft;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.server.Server;
import com.wisc.raft.service.Database;
import com.wisc.raft.service.RaftConsensusService;
import com.wisc.raft.service.ServerClientConnectionService;
import io.grpc.ServerBuilder;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
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
        String levelDB = "./leveldb_"+random.nextInt(100);
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
        ServerClientConnectionService clientConnectionService = new ServerClientConnectionService(raftServer);
        RaftConsensusService raftConsensusService = new RaftConsensusService(raftServer);
        logger.debug("[Sys Args] "+args.toString());
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[1])).addService(raftConsensusService).addService(clientConnectionService).build();

        //Start the server
        raftServer.init();
        server.start();
        server.awaitTermination();
    }
}
