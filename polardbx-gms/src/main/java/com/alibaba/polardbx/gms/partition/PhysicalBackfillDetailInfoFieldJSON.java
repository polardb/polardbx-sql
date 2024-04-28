package com.alibaba.polardbx.gms.partition;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.List;

public class PhysicalBackfillDetailInfoFieldJSON {
    public Pair<String, Integer> sourceHostAndPort;
    public List<Pair<String, Integer>> targetHostAndPorts;
    public long[] bitSet;
    public String msg;

    public PhysicalBackfillDetailInfoFieldJSON() {
    }

    public static PhysicalBackfillDetailInfoFieldJSON fromJson(String json) {
        PhysicalBackfillDetailInfoFieldJSON result = JSON.parseObject(json, PhysicalBackfillDetailInfoFieldJSON.class);
        if (result == null) {
            return new PhysicalBackfillDetailInfoFieldJSON();
        }
        return result;
    }

    public static String toJson(PhysicalBackfillDetailInfoFieldJSON obj) {
        if (obj == null) {
            return "";
        }
        return JSON.toJSONString(obj);
    }

    @Override
    public String toString() {
        return toJson(this);
    }

    public long[] getBitSet() {
        return bitSet;
    }

    public void setBitSet(long[] bitSet) {
        this.bitSet = bitSet;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Pair<String, Integer> getSourceHostAndPort() {
        return sourceHostAndPort;
    }

    public void setSourceHostAndPort(
        Pair<String, Integer> sourceHostAndPort) {
        this.sourceHostAndPort = sourceHostAndPort;
    }

    public List<Pair<String, Integer>> getTargetHostAndPorts() {
        return targetHostAndPorts;
    }

    public void setTargetHostAndPorts(
        List<Pair<String, Integer>> targetHostAndPorts) {
        this.targetHostAndPorts = targetHostAndPorts;
    }

    static public boolean isNotEmpty(PhysicalBackfillDetailInfoFieldJSON extra) {
        if (extra == null) {
            return false;
        }

        return extra.getBitSet() != null && extra.getBitSet().length > 0;
    }
}
