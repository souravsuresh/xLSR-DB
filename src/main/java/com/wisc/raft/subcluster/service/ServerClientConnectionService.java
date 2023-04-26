package com.wisc.raft.subcluster.service;

import com.wisc.raft.subcluster.constants.Role;
import com.wisc.raft.proto.Raft;
import com.wisc.raft.subcluster.server.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.Objects;
import java.util.Optional;

public class ServerClientConnectionService extends ServerClientConnectionGrpc.ServerClientConnectionImplBase{

    private static final Logger logger = LoggerFactory.getLogger(ServerClientConnectionService.class);

    Server server;
    public ServerClientConnectionService(Server server){
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

    @Override
    public void get(Client.Request request, StreamObserver<Client.Response> res){
        long key = request.getKey();
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();
        if (this.server.getState().getNodeType() != Role.LEADER) {
            logger.warn("Cant perform action as this is not leader!!");
            res.onNext(response);
            res.onCompleted();
        }

        else if(commandType.equals("GET")){
            long ret = this.server.getValue(key);
            if(ret != -1){
                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
            }
        }
        res.onNext(response);
        res.onCompleted();
    }


    //@TODO : Probably should Long instead of long
    @Override
    public void put(Client.Request request, StreamObserver<Client.Response> res){
        long key = request.getKey();
        long val = request.getValue();
        String commandType = request.getCommandType();
        Client.Response response = Client.Response.newBuilder().setSuccess(false).setValue(-1).build();
        if (this.server.getState().getNodeType() != Role.LEADER) {
            logger.warn("Cant perform action as this is not leader!!");
            res.onNext(response);
            res.onCompleted();
        }

        if(commandType.equals("PUT")){
            logger.debug("[put] Can perform ");
            String clientKey = request.getEndpoint().getHost() + ":" + request.getEndpoint().getPort();
            int ret = server.putValue(key, val, clientKey);
            if(ret != -1){
                response = Client.Response.newBuilder().setSuccess(true).setValue(ret).build();
            }
        }
        res.onNext(response);
        res.onCompleted();

    }
}
