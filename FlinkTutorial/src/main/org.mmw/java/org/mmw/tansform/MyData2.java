package org.mmw.tansform;

import java.util.Arrays;

public class MyData2 {
    public int keyId;
    public long timestamp;
    public int num;
    public double[] valueList;

    public MyData2() {
    }

    public MyData2(int accountId, long timestamp, int num, double[] valueList) {
        this.keyId = accountId;
        this.timestamp = timestamp;
        this.num = num;
        this.valueList = valueList;
    }

    public long getKeyId() {
        return keyId;
    }

    public void setKeyId(int keyId) {
        this.keyId = keyId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double[] getValueList() {
        return valueList;
    }

    public void setValueList(double[] valueList) {
        this.valueList = valueList;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "MyData{" +
                "keyId=" + keyId +
                ", timestamp=" + timestamp +
                ", num=" + num +
                ", valueList= " + Arrays.toString(valueList) +
                '}';
    }
}

