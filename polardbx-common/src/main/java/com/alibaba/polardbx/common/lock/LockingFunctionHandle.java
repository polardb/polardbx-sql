/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.lock;

import java.sql.SQLException;
import java.util.List;

/**
 * session level
 *
 * @author pangzhaoxing
 */
public interface LockingFunctionHandle {

    /**
     * Tries to obtain a lock with a name given by the string str, using a timeout of timeout seconds. A negative timeout value means infinite timeout.
     * Returns 1 if the lock was obtained successfully, 0 if the attempt timed out (for example, because another client has previously locked the name), or NULL if an error occurred
     */
    Integer tryAcquireLock(String lockName, int timeout);

    /**
     * Releases the lock named by the string str that was obtained with tryAcquireLock().
     * Returns 1 if the lock was released, 0 if the lock was not established by this thread (in which case the lock is not released), and NULL if the named lock did not exist.
     */
    Integer release(String lockName);

    /**
     * Releases all named locks held by the current session
     * returns the number of locks released (0 if there were none)
     */
    Integer releaseAllLocks();

    /**
     * Checks whether the lock named str is free to use (that is, not locked).
     * Returns 1 if the lock is free (no one is using the lock), 0 if the lock is in use, and NULL if an error occurs
     */
    Integer isFreeLock(String lockName);

    /**
     * Checks whether the lock named str is in use (that is, locked).
     * If so, it returns the connection identifier of the client session that holds the lock. Otherwise, it returns NULL.
     */
    String isUsedLock(String lockName);

    /**
     * Checks whether the lock named str is belong to current session.
     * Returns 1 if the lock was valid, 0 if the lock was not valid
     */
    Integer isMyLockValid(String lockName);

    List<String> getAllMyLocks();

}
