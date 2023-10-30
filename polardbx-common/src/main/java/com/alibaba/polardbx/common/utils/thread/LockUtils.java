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

package com.alibaba.polardbx.common.utils.thread;

import com.alibaba.polardbx.common.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

/**
 * @author yaozhili
 */
public class LockUtils {

    public static Collection<String> releaseReadStampLocks(Collection<Pair<StampedLock, Long>> locks) {
        List<String> errorMessages = new ArrayList<>();
        for (Pair<StampedLock, Long> lock : locks) {
            try {
                lock.getKey().unlock(lock.getValue());
            } catch (Throwable t) {
                errorMessages.add("Failed to unlock stamp lock, stamp is " + lock.getValue()
                    + ", caused by " + t.getMessage());
            }
        }
        return errorMessages;
    }
}
