package com.wisc.raft.loadbalancer.dto;

public class ReadLBObject {

    int clusterId;

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public int getReturnVal() {
        return returnVal;
    }

    public void setReturnVal(int returnVal) {
        this.returnVal = returnVal;
    }

    public int getVersionNumber() {
        return versionNumber;
    }

    public void setVersionNumber(int versionNumber) {
        this.versionNumber = versionNumber;
    }

    int returnVal;
    int versionNumber;
}
