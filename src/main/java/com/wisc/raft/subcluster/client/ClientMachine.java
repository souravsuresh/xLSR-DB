package com.wisc.raft.subcluster.client;

import com.wisc.raft.proto.Raft;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.io.IOException;

import static java.lang.Thread.sleep;

public class ClientMachine {
    private static final Logger logger =  LoggerFactory.getLogger(ClientMachine.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        ClientService clientService = new ClientService();
        clientService.setCount(0);
        int appends =  Integer.parseInt(args[2]);
        io.grpc.Server server = ServerBuilder.forPort(Integer.parseInt(args[4])).addService(clientService).build();
        server.start();
        switch (args[6]) {
            case "write":
                write(args);
                break;
            case "read":
                read(args);
                break;
            case "writeread":
                writeread(args);
                break;
            default:
                logger.info("Current supported methods are write/read/writeread");
        }
        server.shutdownNow();
    }

    private static void write(String[] args) throws InterruptedException {
        logger.info(args[0] + " : " + args[1]);
        ManagedChannel LeaderChannel = ManagedChannelBuilder.forAddress(args[0],  Integer.parseInt(args[1])).usePlaintext().build();
        ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStubLeader = ServerClientConnectionGrpc.newBlockingStub(LeaderChannel);
        int numberOfAppends = Integer.parseInt(args[2]);
        Client.Endpoint endpoint = Client.Endpoint.newBuilder().setPort(Integer.parseInt(args[4])).setHost(args[3]).build();
        logger.info("Starting the requests at :: "+ System.currentTimeMillis());
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfAppends; i++) {
            //TODO put to const
            int key = 10000;
            int val = 10000;
            Client.Request request = Client.Request.newBuilder().setCommandType("WRITE").setKey(String.valueOf(key)).setValue(String.valueOf(val)).setEndpoint(endpoint).build();
            try {
                Client.Response response = serverClientConnectionBlockingStubLeader.interact(request);
                if (response.getSuccess()) {

                    logger.debug("Accepted : " + i);
                } else {
                    logger.warn("Failed : " + i + " : ");
                }
            } catch (Exception e) {
                logger.error("Something went wrong : Please check : " + e);
            }

        }
        logger.info("Write Took :: "+ (System.currentTimeMillis() - start));
        logger.info("Finished the requests at :: "+ System.currentTimeMillis());
        logger.info("Total :: "+ (System.currentTimeMillis() - start));
        LeaderChannel.shutdownNow();

    }

    private static void read(String[] args) throws InterruptedException {
        logger.info(args[0] + " : " + args[1]);
        ManagedChannel LeaderChannel = ManagedChannelBuilder.forAddress(args[0],  Integer.parseInt(args[1])).usePlaintext().build();
        ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStubLeader = ServerClientConnectionGrpc.newBlockingStub(LeaderChannel);
        int numberOfAppends = Integer.parseInt(args[2]);
        Client.Endpoint endpoint = Client.Endpoint.newBuilder().setPort(Integer.parseInt(args[4])).setHost(args[3]).build();
        logger.info("Starting the requests at :: "+ System.currentTimeMillis());
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfAppends; i++) {
            //TODO put to const
            int key = 10000;
            int val = 10000;
            Client.Request request = Client.Request.newBuilder().setCommandType("READ").setKey(String.valueOf(key)).setEndpoint(endpoint).build();
            try {
                Client.Response response = serverClientConnectionBlockingStubLeader.interact(request);
                if (response.getSuccess()) {
                    logger.debug("Accepted : " + i + " value :: " + response.getValue());
                } else {
                    logger.warn("Failed : " + i);
                }
            } catch (Exception e) {
                logger.error("Something went wrong : Please check : " + e);
            }

        }
        logger.info("Read Took :: "+ (System.currentTimeMillis() - start));
        logger.info("Finished the requests at :: "+ System.currentTimeMillis());
        LeaderChannel.shutdownNow();
    }


    public static void writeread(String[] args) throws InterruptedException{
        ManagedChannel LeaderChannel = ManagedChannelBuilder.forAddress(args[0],  Integer.parseInt(args[1])).usePlaintext().build();
        ServerClientConnectionGrpc.ServerClientConnectionBlockingStub serverClientConnectionBlockingStubLeader = ServerClientConnectionGrpc.newBlockingStub(LeaderChannel);
        int numberOfAppends = Integer.parseInt(args[2]);
        Client.Endpoint endpoint = Client.Endpoint.newBuilder().setPort(Integer.parseInt(args[4])).setHost(args[3]).build();
        long timer_start =  System.currentTimeMillis();

        for(int i=0;i<numberOfAppends;i++) {
            Client.Request request = Client.Request.newBuilder().setCommandType("WRITE").setKey(String.valueOf(i*10000)).setValue(String.valueOf(i*10000)).setEndpoint(endpoint).build();
            Client.Response response = serverClientConnectionBlockingStubLeader.interact(request);
            logger.info("Write took "+ (System.currentTimeMillis() - timer_start));
            long l = System.currentTimeMillis();

            if (response.getSuccess()) {
                int count = 0;
                while (true) {
                    try {
                        Client.Request read_req = Client.Request.newBuilder().setCommandType("READ").setKey(String.valueOf(i*10000)).setEndpoint(endpoint).build();
                        Client.Response read_response = serverClientConnectionBlockingStubLeader.interact(read_req);
                        if (read_response.getSuccess()) {
                            logger.debug("Accepted : " + i*10000 + " value :: " + read_response.getValue());
                            logger.info("Write Read of "+(i*10000)+" took "+ (System.currentTimeMillis() - l));
                            break;
                        } else {
                            logger.warn("Failed : " + i);
                        }
                    } catch (Exception e) {
                        logger.error("Something went wrong : Please check : " + e);
                    }
                    count++;
                    if (count >= 100) {
                        logger.info("Threshold breached!! Breaking");
                        break;
                    }
                }
            }
        }
        logger.info("Finish : " +  timer_start + " : " + System.currentTimeMillis() + " took:: "+ (System.currentTimeMillis() - timer_start) );

    }
}
