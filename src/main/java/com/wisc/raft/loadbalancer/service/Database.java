package com.wisc.raft.loadbalancer.service;

import com.wisc.raft.proto.Raft;
import lombok.Getter;
import lombok.Setter;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

@Getter
@Setter
public class Database {
    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    private String path;
    private DB db;

    public Database(String path) {
        Options options = new Options();
        options.createIfMissing(true);
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
    public static Raft.LogEntry deserialize(byte[] data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (Raft.LogEntry) is.readObject();
    }

    public int commit(Raft.LogEntry logEntry) {
        // @TODO:: check this
        byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(Long.parseLong(logEntry.getCommand().getKey())).array();
        if (Objects.isNull(keyBytes)) {
            logger.error("[Database] Key cannot not be serialized");
            return -1;
        }
        try {
            byte[] object = serialize(logEntry);
            if (Objects.isNull(keyBytes)) {
                logger.error("[Database] LogEntry cannot not be serialized");
                return 1;
            }
            db.put(keyBytes, object);
            return 0;
        } catch (Exception e) {
            logger.error("[Database] Exception while serializing : " + e + " key" + logEntry);
        }
        return 0;
    }

    public long read(long key) {
        byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(key).array();
        if (Objects.isNull(keyBytes)) {
            logger.error("[Database] Object not retrieved");
            return -1;
        }
        byte[] bytes = db.get(keyBytes);
        try {
            Raft.LogEntry logEntry = deserialize(bytes);
            // @TODO :: check this
            return Long.parseLong(logEntry.getCommand().getValue());
        } catch (Exception e) {
            logger.error("[Database] Exception while deserializing : " + e);
        }
        return 0;
    }
}
