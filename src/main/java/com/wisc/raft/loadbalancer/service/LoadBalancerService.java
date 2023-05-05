package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.loadbalancer.server.LoadBalancerServer;
import com.wisc.raft.proto.LoadBalancerRequestServiceGrpc;
import com.wisc.raft.proto.Loadbalancer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancerService extends LoadBalancerRequestServiceGrpc.LoadBalancerRequestServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerService.class);

    LoadBalancerServer server;

    public LoadBalancerService(LoadBalancerServer server) {
        this.server = server;
    }

    @Override
    public void sendEntries(Loadbalancer.LoadBalancerRequest request,
                            StreamObserver<Loadbalancer.LoadBalancerResponse> responseObserver) {
        logger.debug("[LoadBalancerSendEntries] Request Entries :: " + request.getEntriesList() +
                            " :: Cluster ID -> "+request.getDataClusterId());
        Loadbalancer.LoadBalancerResponse.Builder responseBuilder = Loadbalancer.LoadBalancerResponse.newBuilder();
        this.server.getLock().lock();
        try {
            this.server.getState().getSnapshot().addAll(request.getEntriesList());
            logger.info("[LoadBalancerSendEntries] Snapshot after adding entries :: "+ this.server.getState().getSnapshot());
            AtomicInteger index = new AtomicInteger();
            request.getEntriesList().stream().forEach(ent -> {
                responseBuilder.setSuccess(index.getAndIncrement(), true);
            });
        } catch (Exception ex) {
            logger.debug("Exception found :: "+ex);
            AtomicInteger index = new AtomicInteger();
            request.getEntriesList().stream().forEach(ent -> {
                responseBuilder.setSuccess(index.getAndIncrement(), false);
            });
        } finally {
            this.server.getLock().unlock();
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}