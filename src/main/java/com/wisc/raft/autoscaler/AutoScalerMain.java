package com.wisc.raft.autoscaler;

import com.wisc.raft.subcluster.RaftServer;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AutoScalerMain {
    /**
     *
     * pass the port number for now
     * @param args
     * @throws InterruptedException
     */
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    public static void main(String [] args) throws InterruptedException, IOException {
        logger.debug("[AutoScalerMain] Started ");
        AutoScaleService autoScalerService = new AutoScaleService();
        io.grpc.Server server = ServerBuilder.forPort(9999).addService(autoScalerService).build();
        server.start();
        server.awaitTermination();

    }
}
