package com.wisc.raft.basic;

import com.wisc.raft.proto.Sample;
import com.wisc.raft.proto.SampleServiceGrpc;
import com.wisc.raft.service.Database;
import com.wisc.raft.service.SampleDatabase;
import io.grpc.stub.StreamObserver;
import org.wisc.raft.proto.Client;

import java.util.List;

public class SampleRPCService extends SampleServiceGrpc.SampleServiceImplBase {
    private SampleDatabase db;
    static int count = 0;

    SampleRPCService(SampleDatabase db){
        this.db = db;
    }

    @Override
    public void appendEntries(Sample.SampleAppendEntriesRequest sampleAppendEntriesRequest, StreamObserver<Sample.SampleAppendEntriesResponse> res){
        Sample.SampleLogEntry entries = sampleAppendEntriesRequest.getEntriesList().get(0);
        Sample.SampleAppendEntriesResponse.Builder responseBuilder = Sample.SampleAppendEntriesResponse.newBuilder().setSuccess(false).setTerm(1).setLastMatchTerm(1).setLastMatchIndex(1);
        int commit = db.commit(entries);
        Sample.SampleAppendEntriesResponse response;
        if(commit != -1){
            response = responseBuilder.setSuccess(true).build();
            count++;
            if(count % 50 == 0){
                System.out.println(count);
            }
        }
        else{
            response = responseBuilder.build();
        }
        res.onNext(response);
        res.onCompleted();
    }

}
