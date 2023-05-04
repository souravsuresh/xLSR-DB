package com.wisc.raft.loadbalancer.dto;

import javafx.util.Pair;

import java.util.List;

public class CommitReturnObject {
    List<Pair<String,Integer>> toBeDeleted;
    int retValue;

    public void setRetValue(int retValue) {
        this.retValue = retValue;
    }

    public void setToBeDeleted(List<Pair<String, Integer>> toBeDeleted) {
        this.toBeDeleted = toBeDeleted;
    }

    public List<Pair<String, Integer>> getToBeDeleted() {
        return toBeDeleted;
    }

    public int getRetValue() {
        return retValue;
    }

    public CommitReturnObject(List<Pair<String, Integer>> toBeDeleted, int retValue) {
        this.toBeDeleted = toBeDeleted;
        this.retValue = retValue;
    }

    public CommitReturnObject(int retValue) {
        this.retValue = retValue;
    }
}
