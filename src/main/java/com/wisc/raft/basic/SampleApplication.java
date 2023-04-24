package com.wisc.raft.basic;

import com.wisc.raft.RaftServer;
import com.wisc.raft.proto.Sample;
import com.wisc.raft.service.SampleDatabase;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SampleApplication {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    public static void main(String [] args) throws IOException, InterruptedException {
        SampleDatabase database = new SampleDatabase(args[2]);
        SampleRPCService clientConnectionService = new SampleRPCService(database);
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[1])).addService(clientConnectionService).build();
        server.start();
        server.awaitTermination();

    }
}
