package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.loadbalancer.dto.CommitReturnObject;
import javafx.util.Pair;
import lombok.Getter;
import lombok.Setter;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

@Getter
@Setter
public class LoadBalancerDatabase {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerDatabase.class);
    private String path;
    private DB db;

    ConcurrentLinkedDeque<Pair<String, Integer>> queue;

    int keep = 3;

    public LoadBalancerDatabase(String path) {
        Options options = new Options();
        options.createIfMissing(true);
        queue = new ConcurrentLinkedDeque<>();
        this.path = path;
        try {
            db = factory.open(new File(this.path), options);
        } catch (Exception e) {
            System.out.println("Error in Database creation : " + e);
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
    public static List<Integer> deserialize(byte[] data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (List<Integer>) is.readObject();
    }

    public CommitReturnObject commit(String key, int clusterId) {
        byte[] keyBytes = key.getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[Database] Key cannot not be serialized");
            return new CommitReturnObject(-1);
        }
        try {
            if (Objects.isNull(keyBytes)) {
                logger.error("[Database] LogEntry cannot not be serialized");
                return new CommitReturnObject(-3);
            }
            byte[] bytesValue = db.get(keyBytes);
            List<Integer> currList = deserialize(bytesValue);

            currList.add(clusterId);
            CommitReturnObject commitReturnObject = new CommitReturnObject(1);
            if (currList.size() >= keep) {
                List<Integer> toBeCleaned = currList.subList(0, keep);
                //queue.addAll(toBeCleaned.stream().map(x -> new Pair<String,Integer>(key,x)).collect(Collectors.toList()));
                List<Pair<String, Integer>> collect = toBeCleaned.stream().map(x -> new Pair<String, Integer>(key, x)).collect(Collectors.toList());
                commitReturnObject.setToBeDeleted(collect);
                currList.clear();
            }
            byte[] serialize = serialize(currList);
            db.put(keyBytes, serialize);

            return commitReturnObject;
        } catch (Exception e) {
            logger.error("[Database] Exception while serializing : " + e + " key : " + key);
        }
        return new CommitReturnObject(-2);
    }

    public long read(String key) {
        byte[] keyBytes = key.getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[Database] Object not retrieved");
            return -1;
        }
        byte[] bytes = db.get(keyBytes);
        try {
            List<Integer> listOfVersion = deserialize(bytes);
            return listOfVersion.get(listOfVersion.size() - 1);
        } catch (Exception e) {
            logger.error("[Database] Exception while deserializing : " + e);
        }
        return -2;
    }

    public boolean remove(String key) {
        byte[] keyBytes = key.getBytes();
        if (Objects.isNull(keyBytes)) {
            logger.error("[Database] Object not available for delete");
            return false;
        }
        try {
            db.delete(keyBytes);
        } catch (Exception e) {
            logger.error("[Database] Exception while deleting : " + e);
            return false;
        }
        return true;
    }


}
