package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.loadbalancer.server.LoadBalancerServer;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.constants.Role;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.Objects;
import java.util.Optional;

public class ServerClientConnectionService extends ServerClientConnectionGrpc.ServerClientConnectionImplBase{

    private static final Logger logger = LoggerFactory.getLogger(ServerClientConnectionService.class);

    LoadBalancerServer server;
    public ServerClientConnectionService(LoadBalancerServer server){
        this.server = server;
    }

    @Override
    public void getLeader(Client.MetaDataRequest request, StreamObserver<Client.MetaDataResponse> res){
        String reqString = request.getReqType();
        Client.MetaDataResponse.Builder metaDataResponseBuilder = Client.MetaDataResponse.newBuilder();
        if(reqString.equals("LEADER_CONNECT")){
            if(this.server.getState().getNodeType().equals(Role.LEADER)) {
                logger.debug("[getLeader] : I am the leader, GREAT !");
                Optional<Raft.ServerConnect> leaderOpt = this.server.getCluster().stream().filter(serv -> serv.getServerId() == Integer.parseInt(this.server.getState().getNodeId())).findAny();
                if (leaderOpt.isPresent()) {
                    Raft.ServerConnect serverConnect = leaderOpt.get();
                    Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setServerId(serverConnect.getServerId()).setPort(serverConnect.getEndpoint().getPort()).setHost(serverConnect.getEndpoint().getHost()).setSuccess(true).build();
                    res.onNext(metaDataResponse);
                    res.onCompleted();
                }
            }
            else if(this.server.getState().getNodeType().equals(Role.CANDIDATE) || Objects.isNull(this.server.getState().getVotedFor())){
                logger.debug("[getLeader] : Election going on - Don't disturb");
                Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setSuccess(false).build();
                res.onNext(metaDataResponse);
                res.onCompleted();
            }
            else{

                Optional<Raft.ServerConnect> leaderOpt = this.server.getCluster().stream().filter(serv -> serv.getServerId() == Integer.parseInt(this.server.getState().getVotedFor())).findAny();
                if (leaderOpt.isPresent()) {
                    logger.debug("[getLeader] : FOLLOWER : " + leaderOpt.get());

                    Raft.ServerConnect serverConnect = leaderOpt.get();
                    Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setServerId(serverConnect.getServerId()).setPort(serverConnect.getEndpoint().getPort()).setHost(serverConnect.getEndpoint().getHost()).setSuccess(true).build();
                    res.onNext(metaDataResponse);
                    res.onCompleted();
                }
                else{
                    logger.debug("[getLeader] some config issue : " + this.server.getState().getVotedFor());
                    Client.MetaDataResponse metaDataResponse = metaDataResponseBuilder.setSuccess(false).build();
                    res.onNext(metaDataResponse);
                    res.onCompleted();
                }
            }
        }

    }

    //@TODO : Probably should Long instead of long
    @Override
    public void interact(Client.Request request, StreamObserver<Client.Response> res){
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();
        if (!this.server.getState().getNodeType().equals(Role.LEADER)) {
            logger.warn("Cant perform action as this is not leader!!");
            res.onNext(response);
            res.onCompleted();
        }

        if(commandType.equals("WRITE") || commandType.equals("READ")){
            logger.debug("[interact] Can perform either operation ");
            long ret = server.preCall(request);
            if(ret > 0){
                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
            }
        }
        res.onNext(response);
        res.onCompleted();

    }
}
