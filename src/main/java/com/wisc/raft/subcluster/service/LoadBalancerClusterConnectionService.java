package com.wisc.raft.subcluster.service;

import com.wisc.raft.proto.Cluster;
import com.wisc.raft.proto.LeaderServiceGrpc;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.constants.Role;
import com.wisc.raft.subcluster.server.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public class LoadBalancerClusterConnectionService extends LeaderServiceGrpc.LeaderServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerClusterConnectionService.class);

    Server server;

    @Override
    public void getLeaderConnect(Cluster.RequestLeaderRPC request, StreamObserver<Cluster.ResponseLeaderRPC> response) {
        String leaderId = request.getLeaderId();
        Cluster.ResponseLeaderRPC.Builder responseLeaderBuilder = Cluster.ResponseLeaderRPC.newBuilder();
        if (leaderId.equals(server.getState().getLeaderId())) {
            logger.info("[LoadBalancerClusterConnectionService] Matched us");
            Optional<Raft.ServerConnect> leaderOpt = this.server.getCluster().stream().filter(serv -> serv.getServerId() == Integer.parseInt(leaderId)).findAny();
            if (leaderOpt.isPresent()) {
                Raft.ServerConnect serverConnect = leaderOpt.get();
                Cluster.ClusterEndpoint clusterEndpoint = Cluster.ClusterEndpoint.newBuilder().setHost(serverConnect.getEndpoint().getHost()).setPort(serverConnect.getEndpoint().getPort()).build();
                responseLeaderBuilder.setRetValue(0).setClusterEndpoint(clusterEndpoint);
            } else {
                logger.error("[LoadBalancerClusterConnectionService] this cannot happen");
                responseLeaderBuilder.setRetValue(-3);
            }

        } else if (server.getState().getNodeType().equals(Role.CANDIDATE) || Objects.isNull(this.server.getState().getVotedFor())) {
            logger.info("[LoadBalancerClusterConnectionService] Still a Candidate");
            responseLeaderBuilder.setRetValue(-1);
        } else if (server.getState().getNodeType().equals(Role.FOLLOWER)) {
            logger.info("[LoadBalancerClusterConnectionService] I am  a Follower");
            responseLeaderBuilder.setRetValue(-2);
            Optional<Raft.ServerConnect> leaderOpt = this.server.getCluster().stream().filter(serv -> serv.getServerId() == Integer.parseInt(this.server.getState().getVotedFor())).findAny();
            if (leaderOpt.isPresent()) {
                Raft.ServerConnect serverConnect = leaderOpt.get();
                Cluster.ClusterEndpoint clusterEndpoint = Cluster.ClusterEndpoint.newBuilder().setHost(serverConnect.getEndpoint().getHost()).setPort(serverConnect.getEndpoint().getPort()).build();
                responseLeaderBuilder.setClusterEndpoint(clusterEndpoint);
            }
        }

        response.onNext(responseLeaderBuilder.build());
        response.onCompleted();


    }

}
