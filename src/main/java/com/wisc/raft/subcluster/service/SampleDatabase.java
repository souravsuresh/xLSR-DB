package com.wisc.raft.subcluster.service;

import com.wisc.raft.proto.Sample;
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
public class SampleDatabase {
    private static final Logger logger = LoggerFactory.getLogger(SampleDatabase.class);

    private String path;
    private DB db;

    public SampleDatabase(String path) {
        Options options = new Options();
        options.createIfMissing(true);
        this.path = path;
        try {
            db = factory.open(new File(this.path), options);
        } catch (Exception e) {
            System.out.println("Error in Database creation : " + e);
        }
    }

    public static byte[] serialize(Sample.SampleLogEntry obj) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        os.flush();
        return out.toByteArray();
    }

    // Deserialize a byte array to an object
    public static Sample.SampleLogEntry deserialize(byte[] data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (Sample.SampleLogEntry) is.readObject();
    }

    public int commit(Sample.SampleLogEntry logEntry) {
        byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(logEntry.getCommand().getKey()).array();
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
            Sample.SampleLogEntry logEntry = deserialize(bytes);
            return logEntry.getCommand().getValue();
        } catch (Exception e) {
            logger.error("[Database] Exception while deserializing : " + e);
        }
        return 0;
    }
}
