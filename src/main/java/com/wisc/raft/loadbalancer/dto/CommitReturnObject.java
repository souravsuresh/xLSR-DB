package com.wisc.raft.loadbalancer.dto;

import javafx.util.Pair;

import java.util.List;

public class CommitReturnObject {
    List<Pair<Integer,Integer>> toBeDeleted;

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    int clusterId;
    int retValue;

    public void setRetValue(int retValue) {
        this.retValue = retValue;
    }

    public void setToBeDeleted(List<Pair<Integer, Integer>> toBeDeleted) {
        this.toBeDeleted = toBeDeleted;
    }

    public List<Pair<Integer, Integer>> getToBeDeleted() {
        return toBeDeleted;
    }

    public int getRetValue() {
        return retValue;
    }

    public CommitReturnObject(List<Pair<Integer, Integer>> toBeDeleted, int retValue) {
        this.toBeDeleted = toBeDeleted;
        this.retValue = retValue;
    }

    public CommitReturnObject(int retValue) {
        this.retValue = retValue;
    }
}
