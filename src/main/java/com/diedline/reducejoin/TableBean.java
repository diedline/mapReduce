package com.diedline.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    private String oldId;
    private String pid;
    private int amount;
    private String pName;
    private String flag;


    public TableBean() {
    }

    @Override
    public String toString() {
        return oldId + "\t" + pName + "\t" + amount;
    }

    public String getOldId() {
        return oldId;
    }

    public void setOldId(String oldId) {
        this.oldId = oldId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(oldId);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        oldId = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readInt();
        pName = dataInput.readUTF();
        flag = dataInput.readUTF();
    }
}
