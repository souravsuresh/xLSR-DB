package com.wisc.raft.subcluster.storagestate;

import com.wisc.raft.subcluster.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageState {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    String dirPath;
    float threshold;

    public String getDirPath() {
        return dirPath;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

    public float getThreshold() {
        return threshold;
    }

    public void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    public StorageState(String dirPath, float threshold){
        this.dirPath = dirPath;
        this.threshold = threshold;
    }

    public float getUtilization(){
        Path directory = Paths.get(this.dirPath);
        try {
            if (!Files.exists(directory)) {
                Files.createDirectories(directory);
            }
            long size = Files.walk(directory)
                    .mapToLong(p -> p.toFile().length())
                    .sum(); //Bytes

            logger.debug("Size of " + directory + " is " + size + " bytes");

            if(size <= this.threshold){
                logger.debug("Looking GOOD !");

            }
            else{
                logger.error("Overflowing help me");
                return -2;

            }
            return (size/threshold)*100;
        } catch (Exception e) {
            logger.error("Error: " + e.getMessage());
        }
        return -1;
    }
}
