package com.alibaba.polardbx.executor.ddl.newengine.resource;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.HashMap;
import java.util.Map;

public class ResourceContainer {
    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Map<String, Pair<Long, Long>> getOwnerMap() {
        return ownerMap;
    }

    public void setOwnerMap(
        Map<String, Pair<Long, Long>> ownerMap) {
        this.ownerMap = ownerMap;
    }

    long amount;
    String owner;
    // ownerList: owner => (amount, shares)
    Map<String, Pair<Long, Long>> ownerMap;

    ResourceContainer() {
        this.amount = 0;
        this.owner = null;
        this.ownerMap = new HashMap<>();
    }

    ResourceContainer(Long amount, String owner) {
        this.amount = amount;
        this.owner = owner;
        this.ownerMap = new HashMap<>();
    }

    @JSONCreator
    ResourceContainer(Long amount, String owner, Map<String, Pair<Long, Long>> ownerMap) {
        this.amount = amount;
        this.owner = owner;
        this.ownerMap = ownerMap;
    }

    public Boolean allocate(ResourceContainer ddlEngineResource) {
        Boolean allocated = false;
        if (ddlEngineResource.owner == null) {
            amount -= ddlEngineResource.amount;
            allocated = true;
        } else if (!ownerMap.containsKey(ddlEngineResource.owner)) {
            amount -= ddlEngineResource.amount;
            ownerMap.put(ddlEngineResource.owner, Pair.of(ddlEngineResource.amount, 1L));
            allocated = true;
        } else {
            Pair<Long, Long> pair = ownerMap.get(ddlEngineResource.owner);
            Pair<Long, Long> newPair = Pair.of(ddlEngineResource.amount, pair.getValue() + 1);
            amount -= ddlEngineResource.amount - pair.getKey();
            ownerMap.put(ddlEngineResource.owner, newPair);
            allocated = true;
        }
        return allocated;
    }

    @Override
    public String toString() {
        if (ownerMap == null || ownerMap.isEmpty()) {
            return String.format("[amount=%d,owner=%s]", amount, owner);
        } else {
            return String.format("[amount=%d,ownerMap=%s]", amount, ownerMap.toString());
        }
    }

    public Boolean cover(ResourceContainer ddlEngineResource) {
        Boolean covered = false;
        if (ddlEngineResource.owner == null || !ownerMap.containsKey(ddlEngineResource.owner)) {
            if (amount >= ddlEngineResource.amount) {
                covered = true;
            }
        } else {
            if (amount >= ddlEngineResource.amount - ownerMap.get(ddlEngineResource.owner).getKey()) {
                covered = true;
            }
        }
        return covered;
    }

    public void free(ResourceContainer ddlEngineResource) {
        if (ddlEngineResource.owner == null) {
            amount += ddlEngineResource.amount;
        } else {
            Pair<Long, Long> pair = ownerMap.get(ddlEngineResource.owner);
            if (pair.getValue() <= 1) {
                amount += pair.getKey();
                ownerMap.remove(ddlEngineResource.owner);
            } else {
                ownerMap.put(ddlEngineResource.owner, Pair.of(pair.getKey(), pair.getValue() - 1));
            }
        }
    }
}
