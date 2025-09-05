package com.alibaba.polardbx.executor.ddl.newengine.resource;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.HashMap;
import java.util.Map;

public class ResourceContainer {
    public static int NO_PHASE_LOCK = 0;
    public static int PHASE_LOCK_START = 1;
    public static int PHASE_LOCK_END = 2;

    //    ----------------------PHASE_LOCK 1
    // subjob
//    ----------------------acquire(PHASE_LOCK_START)    ----------- release(PHASE_LOCK_START)
//                          -1 with owner set                        +1 with owner check
//     checker
//    ----------------------acquire(PHASE_LOCK_END)     ------------ release(PHASE_LOCK_END)
//                          +1 with owner check                      do nothing.
//    ----------------------release(PHASE_LOCK_END)      ----------  acquire(PHASE_LOCK_END)
//                          do nothing                               +1 with owner check

    //    ----------------------release(PHASE_LOCK_START)
//                          +1 with owner check
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

    public Boolean getForce() {
        return force;
    }

    public void setForce(Boolean force) {
        this.force = force;
    }

    Boolean force = false;

    public int getPhaseLock() {
        return phaseLock;
    }

    public void setPhaseLock(int phaseLock) {
        this.phaseLock = phaseLock;
    }

    int phaseLock = 0;

    ResourceContainer(Long amount, String owner) {
        this.amount = amount;
        this.owner = owner;
        this.ownerMap = new HashMap<>();
        this.force = false;
        this.phaseLock = NO_PHASE_LOCK;
    }

    ResourceContainer(Long amount, String owner, Boolean force) {
        this.amount = amount;
        this.owner = owner;
        this.ownerMap = new HashMap<>();
        this.force = force;
        this.phaseLock = NO_PHASE_LOCK;
    }

    ResourceContainer(Long amount, String owner, Boolean force, int phaseLock) {
        this.amount = amount;
        this.owner = owner;
        this.ownerMap = new HashMap<>();
        this.force = force;
        this.phaseLock = phaseLock;
    }

    @JSONCreator
    ResourceContainer(Long amount, String owner, Map<String, Pair<Long, Long>> ownerMap, Boolean force, int phaseLock) {
        this.amount = amount;
        this.owner = owner;
        this.ownerMap = ownerMap;
        this.force = force;
        this.phaseLock = phaseLock;
    }

    public Boolean allocate(ResourceContainer ddlEngineResource) {
        Boolean allocated = false;
        if (ddlEngineResource.phaseLock == PHASE_LOCK_START && owner == null) {
            amount -= ddlEngineResource.amount;
            owner = ddlEngineResource.owner;
            ownerMap.put(ddlEngineResource.owner, Pair.of(ddlEngineResource.amount, 1L));
            allocated = true;
        } else if (ddlEngineResource.phaseLock == PHASE_LOCK_END) {
            allocated = true;
            if (owner != null && owner.equalsIgnoreCase(ddlEngineResource.owner)) {
                amount += ddlEngineResource.amount;
                owner = null;
                ownerMap.remove(ddlEngineResource.owner);
            }
        } else if (ddlEngineResource.owner == null) {
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
        if (ddlEngineResource.phaseLock == PHASE_LOCK_START) {
            if (amount >= ddlEngineResource.amount) {
                covered = true;
            }
        } else if (ddlEngineResource.phaseLock == PHASE_LOCK_END) {
            covered = true;
        } else if (ddlEngineResource.owner == null || !ownerMap.containsKey(ddlEngineResource.owner)) {
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
        if (ddlEngineResource.phaseLock == PHASE_LOCK_START) {
            if (owner != null && owner.equalsIgnoreCase(ddlEngineResource.owner)) {
                amount += ddlEngineResource.amount;
                owner = null;
                ownerMap.remove(ddlEngineResource.owner);
            }
        } else if (ddlEngineResource.phaseLock == PHASE_LOCK_END) {
            // do nothing.
        } else if (ddlEngineResource.owner == null) {
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
