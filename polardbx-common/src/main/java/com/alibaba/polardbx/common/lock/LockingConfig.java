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

/**
 * @author pangzhaoxing
 */
public class LockingConfig {

    /**
     * the maximum number of the locks. let heartbeat delay = 50ms, heartbeat interval = 2s, max_number_of_locks < 2s/50ms
     */
    public static final int MAX_LOCKS_NUMBER = 64;

    /**
     * the heart_beat to maintain the lock (in millisecond).
     */
    public static final int HEART_BEAT_INTERVAL = 10000;

    public static final int MAX_RETRY_TIMES = 3;

    /**
     * 租约机制
     * the expiration time (in second).
     * if expiration_time is too small, the lock will be vulnerable to network delay.
     * but if it is too large, the invalid lock will not be replaced just in time.
     */
    public static final int EXPIRATION_TIME = 60;

}
