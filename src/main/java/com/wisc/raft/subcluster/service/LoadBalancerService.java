package com.wisc.raft.subcluster.service;

import com.wisc.raft.loadbalancer.constants.Role;
import com.wisc.raft.proto.LoadBalancerRequestServiceGrpc;
import com.wisc.raft.proto.Loadbalancer;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.server.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class LoadBalancerService extends LoadBalancerRequestServiceGrpc.LoadBalancerRequestServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerService.class);

    Server server;



    public LoadBalancerService(Server server) {
        this.server = server;
    }
    @Override
    public void getEntries(Loadbalancer.DataReadRequestObject request, StreamObserver<Loadbalancer.DataReadResponseObject> responseObserver) {
        logger.debug("[LoadBalancerSendEntries] Request Key :: " + request.getKey() + " -> "+ request.getVersion()) ;
        Loadbalancer.DataReadResponseObject.Builder responseBuilder = Loadbalancer.DataReadResponseObject.newBuilder();
        this.server.getLock().lock();
        try {
            String s = server.getDb().read(request.getKey() + "_" + request.getVersion()).get();
            if(s == null && request.getTimestamp().compareTo(String.valueOf(server.getState().getHeartbeatTrackerTime() +  5 * 80 * 3)) > 0) {
                responseBuilder.setReturnCode(-1);  // scenario of ghost to write
            } else {
                responseBuilder.setValue(s);
                responseBuilder.setReturnCode(0);
            }

        } catch (Exception ex) {
            logger.debug("Exception found :: "+ex);
        } finally {
            this.server.getLock().unlock();
        }
        responseBuilder.build();
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
    @Override
    public void sendEntries(Loadbalancer.LoadBalancerRequest request,
                            StreamObserver<Loadbalancer.LoadBalancerResponse> responseObserver) {
        logger.debug("[LoadBalancerSendEntries] Request Entries :: " + request.getEntriesList() +
                            " :: Cluster ID -> "+request.getDataClusterId());
        Loadbalancer.LoadBalancerResponse.Builder responseBuilder = Loadbalancer.LoadBalancerResponse.newBuilder();
        this.server.getLock().lock();
        try {
//            if(!this.server.getState().getNodeType().equals(Role.LEADER)) {
//                logger.info("[LoadBalancerSendEntries] Not sending to leader so rejecting it! Node Type ::" +this.server.getState().getNodeType() + " : " + this.server.getState().getNodeType().equals(Role.LEADER));
//                request.getEntriesList().stream().forEach(ent -> {
//                    responseBuilder.addSuccess(false);
//                });
//                return;
//            }
            List<Raft.LogEntry> entriesList = request.getEntriesList();
            for(Raft.LogEntry entry : entriesList){
                String command = entry.getCommand().getCommandType();
                Raft.Command pairObject = entry.getCommand();
                String key = pairObject.getKey();
                if(command.equals("WRITE")){
                    server.putValue(entry);
                }
                else if(command.equals("READ")){
                    server.getValue(key);
                }
            }
            //this.server.getState().getSnapshot().addAll(request.getEntriesList());

            logger.info("[LoadBalancerSendEntries] Snapshot after adding entries :: "+ this.server.getState().getSnapshot());
            request.getEntriesList().stream().forEach(ent -> {
                responseBuilder.addSuccess(true);
            });
        } catch (Exception ex) {
            logger.debug("Exception found :: "+ex);
            request.getEntriesList().stream().forEach(ent -> {
                responseBuilder.addSuccess(false);
            });
        } finally {
            this.server.getLock().unlock();
            responseBuilder.build();
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

    }
}
