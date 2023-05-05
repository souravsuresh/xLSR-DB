package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.loadbalancer.dto.ReadLBObject;
import com.wisc.raft.proto.Raft;
import javafx.util.Pair;
import lombok.Getter;
import lombok.Setter;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

@Getter
@Setter
public class LoadBalancerDatabase {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerDatabase.class);
    private String path;
    private DB db;

    ConcurrentHashMap<byte[], Integer> concurrentCleanUpList;
    int keep = 3;

    public LoadBalancerDatabase(String path) {
        Options options = new Options();
        options.createIfMissing(true);
        concurrentCleanUpList = new ConcurrentHashMap<>();
        this.path = path;
        try {
            db = factory.open(new File(this.path), options);
        } catch (Exception e) {
            System.out.println("[LbDatabase] Error in Database creation : " + e);
        }
    }

    public static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        os.flush();
        return out.toByteArray();
    }

    // Deserialize a byte array to an object
    public static List<Pair<Integer, Integer>> deserialize(byte[] data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (List<Pair<Integer, Integer>>) is.readObject();
    }

    public int commit(Raft.LogEntry logEntry) {
        byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(logEntry.getCommand().getKey()).array();
        if (Objects.isNull(keyBytes)) {
            logger.error("[LbDatabase] Key cannot not be serialized");
            return -1;
        }
        try {
            byte[] object = serialize(logEntry);
            if (Objects.isNull(keyBytes)) {
                logger.error("[LbDatabase] LogEntry cannot not be serialized");
                return 1;
            }
            db.put(keyBytes, object);
            return 0;
        } catch (Exception e) {
            logger.error("[LbDatabase] Exception while serializing : " + e + " key" + logEntry);
        }
        return 0;
    }



    //Returns ClusterID
    public ReadLBObject read(String key) {
        ReadLBObject readLBObject = new ReadLBObject();
        byte[] keyBytes = key.getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[LbDatabase] Object not retrieved");
            readLBObject.setReturnVal(-1);
            return readLBObject;
        }
        byte[] bytes = db.get(keyBytes);
        if(bytes == null){
            logger.error("[LbDatabase] No key exists " );
            readLBObject.setReturnVal(-2);
            readLBObject.setVersionNumber(1);
            return readLBObject;
        }
        try {
            List<Pair<Integer, Integer>> listOfVersion = deserialize(bytes);
            if(listOfVersion.isEmpty()){
                readLBObject.setReturnVal(-2);
                readLBObject.setVersionNumber(1);
                return readLBObject;
            }
            if(listOfVersion.size() > keep){
                concurrentCleanUpList.put(keyBytes,0);
            }
            readLBObject.setReturnVal(0);
            readLBObject.setClusterId(listOfVersion.get(listOfVersion.size() - 1).getKey());
            readLBObject.setVersionNumber(listOfVersion.get(listOfVersion.size() - 1).getValue());

            return readLBObject;
        } catch (Exception e) {
            logger.error("[LbDatabase] Exception while deserializing : " + e);
        }
        readLBObject.setReturnVal(-3);
        return readLBObject;
    }

    public boolean remove(String key) {
        byte[] keyBytes = key.getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[LbDatabase] Object not available for delete");
            return false;
        }
        try {
            db.delete(keyBytes);
        } catch (Exception e) {
            logger.error("[LbDatabase] Exception while deleting : " + e);
            return false;
        }
        return true;
    }

    public void cleanUp(){
        concurrentCleanUpList.forEach((key, value) -> {
            try {
                byte[] bytes = db.get(key);
                List<Pair<Integer, Integer>> valList = deserialize(bytes);
                valList.subList(valList.size() - keep, valList.size());
                db.put(key, serialize(valList));
                concurrentCleanUpList.remove(key);
            }
            catch(Exception e){
                logger.error("[LbDatabase] Exception while deleting : " + e);
            }
        });
    }



}
