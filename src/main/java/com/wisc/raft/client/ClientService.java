package com.wisc.raft.client;

import com.google.common.util.concurrent.CycleDetectingLockFactory;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.Setter;
import org.wisc.raft.proto.Client;
import org.wisc.raft.proto.ServerClientConnectionGrpc;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
public class ClientService extends ServerClientConnectionGrpc.ServerClientConnectionImplBase{
    public int count;
    private Lock lock = new ReentrantLock();

    @Override
    public void talkBack(Client.StatusUpdate req, StreamObserver<Empty> response){
        lock.lock();
        if (req.getReturnVal()) {
           this.setCount(this.getCount() + 1);
        }
        lock.unlock();
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }
}
