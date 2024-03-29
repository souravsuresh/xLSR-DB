package com.wisc.raft.subcluster.service;

import com.wisc.raft.subcluster.constants.Role;
import com.wisc.raft.proto.LoadBalancerRequestServiceGrpc;
import com.wisc.raft.proto.Loadbalancer;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.server.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;


public class LoadBalancerService extends LoadBalancerRequestServiceGrpc.LoadBalancerRequestServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerService.class);

    Server server;



    public LoadBalancerService(Server server) {
        this.server = server;
    }
    @Override
    public void getEntries(Loadbalancer.DataReadRequestObject request, StreamObserver<Loadbalancer.DataReadResponseObject> responseObserver) {
        logger.debug("[LoadBalancerGetEntries] Request Key :: " + request.getKey() + " -> "+ request.getVersion()) ;
        Loadbalancer.DataReadResponseObject.Builder responseBuilder = Loadbalancer.DataReadResponseObject.newBuilder();
        this.server.getLock().lock();
        try {
            String key = request.getKey() + "_" + request.getVersion();
            Optional<String> s = server.getDb().read(key);
            if(!s.isPresent() && (Timestamp.valueOf(request.getTimestamp()).getTime() - System.currentTimeMillis()) > 10000) {
                logger.debug("[LoadBalancerGetEntries] Key with version not found in data cluster :: "+key);
                responseBuilder.setReturnCode(-2);  // scenario of ghost to write
            }
            else if(!s.isPresent()) {
                logger.debug("[LoadBalancerGetEntries] Key with version not found in data cluster :: "+key);
                responseBuilder.setReturnCode(-1);      // key with version not there

            }
            else {
                logger.debug("[LoadBalancerGetEntries] Key with version found in data cluster with value :: "+s.get());  // @TODO :: remove this log!
                responseBuilder.setValue(s.get());
                responseBuilder.setReturnCode(0);
            }
            responseBuilder.build();
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception ex) {
            logger.debug("Exception found :: "+ex);
        } finally {
            this.server.getLock().unlock();
        }


    }
    @Override
    public void sendEntries(Loadbalancer.LoadBalancerRequest request,
                            StreamObserver<Loadbalancer.LoadBalancerResponse> responseObserver) {
        logger.debug("[LoadBalancerSendEntries] Request Entries :: " + request.getEntriesList() +
                            " :: Cluster ID -> "+request.getDataClusterId());
        Loadbalancer.LoadBalancerResponse.Builder responseBuilder = Loadbalancer.LoadBalancerResponse.newBuilder();
        this.server.getLock().lock();
        try {
            if(this.server.getState().getNodeType() != Role.LEADER) {
                logger.info("[LoadBalancerSendEntries] Not sending to leader so rejecting it!");
                request.getEntriesList().forEach(ent -> {
                    responseBuilder.addSuccess(false);
                });
                return;
            }
            List<Raft.LogEntry> entriesList = request.getEntriesList();
            for(Raft.LogEntry entry : entriesList){
                String command = entry.getCommand().getCommandType();
                Raft.Command pairObject = entry.getCommand();
                String key = pairObject.getKey();
                if(command.equals("WRITE")){
                    server.putValue(entry);
                }
            }
            logger.debug("[LoadBalancerSendEntries] Snapshot after adding entries :: "+ this.server.getState().getSnapshot());
            request.getEntriesList().forEach(ent -> {
                responseBuilder.addSuccess(true);
            });
        } catch (Exception ex) {
            logger.debug("Exception found :: "+ex);
            request.getEntriesList().forEach(ent -> {
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
