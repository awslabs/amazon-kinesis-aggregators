package com.amazonaws.services.kinesis.aggregators;

public class InventoryStatus {
    private String lastTime, lowSeq, highSeq;

    public InventoryStatus(String lastTime, String lowSeq, String highSeq) {
        super();
        this.lastTime = lastTime;
        this.lowSeq = lowSeq;
        this.highSeq = highSeq;
    }

    public String getLastTime() {
        return lastTime;
    }

    public String getLowSeq() {
        return lowSeq;
    }

    public String getHighSeq() {
        return highSeq;
    }
}
