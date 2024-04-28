package com.alibaba.polardbx.common.lock;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class EmptyLockingFunctionHandle implements LockingFunctionHandle {

    public Integer tryAcquireLock(String lockName, int timeout) {
        return 0;
    }

    public Integer release(String lockName) {
        return 0;
    }

    public Integer releaseAllLocks() {
        return 0;
    }

    public Integer isFreeLock(String lockName) {
        return 0;
    }

    public String isUsedLock(String lockName) {
        return null;
    }

    public Integer isMyLockValid(String lockName) {
        return 0;
    }

    @Override
    public List<String> getAllMyLocks() {
        return null;
    }
}
