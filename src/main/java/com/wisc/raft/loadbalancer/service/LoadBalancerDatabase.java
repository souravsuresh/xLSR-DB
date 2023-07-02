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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

@Getter
@Setter
public class LoadBalancerDatabase {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerDatabase.class);
    private String path;
    private DB db;

    public HashMap<String, Pair<Integer, Integer>> getCacheEntry() {
        return cacheEntry;
    }

    public void setCacheEntry(HashMap<String, Pair<Integer,Integer>> cacheEntry) {
        this.cacheEntry = cacheEntry;
    }

    HashMap<String, Pair<Integer, Integer>> cacheEntry;
    HashMap<byte[], Integer> concurrentCleanUpList;
    int keep = 3;

    public LoadBalancerDatabase(String path) {
        Options options = new Options();
        options.createIfMissing(true);
        concurrentCleanUpList = new HashMap<>();
        cacheEntry = new HashMap<>();
        this.path = path;
        try {
            db = factory.open(new File(this.path), options);
        } catch (Exception e) {
           logger.error("[LbDatabase] Error in Database creation : " + e);
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
        byte[] keyBytes = logEntry.getCommand().getKey().getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[LbDatabase] Key cannot not be serialized");
            return -1;
        }
        try {
            List<Pair<Integer, Integer>> listOfVersion = readPair(logEntry.getCommand().getKey());
            listOfVersion.add(new Pair<>(logEntry.getCommand().getVersion(), logEntry.getClusterId()));
            byte[] object = serialize(listOfVersion);
            logger.debug("[commit] removed entry : " + logEntry.getCommand().getKey());
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


    private List<Pair<Integer, Integer>> readPair(String key) {
        ReadLBObject readLBObject = new ReadLBObject();
        byte[] keyBytes = key.getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[LbDatabase] Object not retrieved");
            readLBObject.setReturnVal(-1);
            return new ArrayList<>();
        }
        byte[] bytes = db.get(keyBytes);
        if(bytes == null){
            logger.debug("[LbDatabase] No value exists " );
            readLBObject.setReturnVal(2);
            readLBObject.setVersionNumber(0);
            return new ArrayList<>();
        }
        try {
            List<Pair<Integer, Integer>> listOfVersion = deserialize(bytes);
            if(listOfVersion.isEmpty()){
                readLBObject.setReturnVal(2);
                readLBObject.setVersionNumber(0);
                return new ArrayList<>();
            }
            return listOfVersion;
        } catch (Exception e) {
            logger.error("[LbDatabase] Exception while deserializing : " + e);
        }
        readLBObject.setReturnVal(-3);
        return new ArrayList<>();
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
            logger.error("[LbDatabase] No value exists " );
            readLBObject.setReturnVal(2);
            readLBObject.setVersionNumber(0);
            return readLBObject;
        }
        try {
            List<Pair<Integer, Integer>> listOfVersion = deserialize(bytes);
            logger.info("[read] list : + " + listOfVersion);
            if(listOfVersion.isEmpty()){
                logger.error("[LbDatabase] No value exists in the list " );

                readLBObject.setReturnVal(2);
                readLBObject.setVersionNumber(0);

                return readLBObject;
            }
            if(listOfVersion.size() > keep){
                concurrentCleanUpList.put(keyBytes,0);
            }
            readLBObject.setReturnVal(0);
            readLBObject.setClusterId(listOfVersion.get(listOfVersion.size() - 1).getValue());
            readLBObject.setVersionNumber(listOfVersion.get(listOfVersion.size() - 1).getKey());

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
