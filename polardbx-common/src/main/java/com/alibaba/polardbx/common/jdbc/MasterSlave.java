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

package com.alibaba.polardbx.common.jdbc;

public enum MasterSlave {

    MASTER_ONLY,

    /**
     * 优先路由备库，没有任何备库或者备库延迟较大时，路由到主库
     */
    SLAVE_FIRST,

    READ_WEIGHT,

    /**
     * 只路由备库，但不会路由到主库
     */
    SLAVE_ONLY,

    /**
     * 只路由低延迟的备库，路由不到直接报错
     */
    LOW_DELAY_SLAVE_ONLY,

    /**
     * 当开启follower read的时候：
     * 1. 指定follower hint强制路由给follower
     * 2. 不指定follower hint的时候:
     * a）有只读实例的时候，忽略follower read
     * b) 没有只读实例的时候，按照读写分离逻辑路由给follower
     */
    FOLLOWER_ONLY;

    MasterSlave() {
        mask = (1 << ordinal());
    }

    public final int mask;

    public static boolean isEnabled(int features, MasterSlave feature) {
        return (features & feature.mask) != 0;
    }

    public static int config(int features, MasterSlave feature, boolean state) {
        if (state) {
            features |= feature.mask;
        } else {
            features &= ~feature.mask;
        }

        return features;
    }

    public static int of(MasterSlave... features) {
        if (features == null) {
            return 0;
        }

        int value = 0;

        for (MasterSlave feature : features) {
            value |= feature.mask;
        }

        return value;
    }
}
