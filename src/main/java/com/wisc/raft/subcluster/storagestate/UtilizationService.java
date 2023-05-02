package com.wisc.raft.subcluster.storagestate;

import com.wisc.raft.proto.Raft;
import com.wisc.raft.proto.UtilizationServiceGrpc;
import com.wisc.raft.subcluster.constants.Role;
import com.wisc.raft.subcluster.server.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilizationService extends UtilizationServiceGrpc.UtilizationServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(UtilizationService.class);

    public UtilizationService(Server server, StorageState storageState) {
        this.server = server;
        this.storageState = storageState;
    }

    Server server;
    StorageState storageState;

    @Override
    public void getUtilization(Raft.UtilizationRequest request, StreamObserver<Raft.UtilizationResponse> responseObserver) {
        logger.debug("[UtilizationService] Inside getGetUtilization Service Call for :: " + server.getState().getNodeId() + " and requested" +
                " leader is : " + request.getLeaderId());
        if (server.getState().getLeaderId().equals(request.getLeaderId())) {
            Raft.UtilizationResponse.Builder responseBuilder = Raft.UtilizationResponse.newBuilder()
                    .setLeaderId(server.getState().getLeaderId());
            float num = storageState.getUtilization();
            if (num == -1) {
                logger.debug("[UtilizationService] Error occurred during calculation");
                responseBuilder.setReturnVal(-2);
            }
            if (num == -2) {
                logger.debug("[UtilizationService] Threshold Breached");
                responseBuilder.setReturnVal(-3);
            } else {
                responseBuilder.setReturnVal(1).setUtilizationPercentage(num);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
        //else if Not candidate""
        else {
            Raft.UtilizationResponse.Builder responseBuilder = Raft.UtilizationResponse.newBuilder().setReturnVal(-1);

            if (server.getState().getNodeType().equals(Role.CANDIDATE)) {
                logger.debug("[UtilizationService] Contacted wrong Node, Hit a candidate");
                responseBuilder.setReturnVal(-4);
            } else if (server.getState().getNodeType().equals(Role.FOLLOWER)) {
                logger.debug("[UtilizationService] Contacted wrong Node, Hit a Follower");

                Raft.ServerConnect serverConnect = server.getCluster().get(Integer.parseInt(server.getState().getVotedFor())); //Voter ID
                Raft.Endpoint endpoint = serverConnect.getEndpoint();
                responseBuilder.setEndpoint(endpoint).setLeaderId(server.getState().getVotedFor()).setReturnVal(-2);

            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        }
    }

}
