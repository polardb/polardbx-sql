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

package com.alibaba.polardbx.optimizer.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaVersionManager {
    static public int segmentLockSize = 64;

    public static Map<String, Counter> schemaVersions = new ConcurrentHashMap<>();

    public static void increaseSchemaVersion(String schemaName) {
        Counter counter = schemaVersions.computeIfAbsent(schemaName, x -> new Counter());
        counter.increment();
    }

    public static String getSchemaDigest(String schemaName) {
        Counter counter = schemaVersions.computeIfAbsent(schemaName, x -> new Counter());
        long version = counter.getValue();
        return String.format("schema:%s#version:%d", schemaName, version);
    }

    public static List<String> getSchemaDigestList(String schemaName) {
        Counter counter = schemaVersions.computeIfAbsent(schemaName, x -> new Counter());

        String tableGroupDigest = String.format("schema:%s#version:%d", schemaName, counter.getValue());

        List<String> tableGroupDigestList = new ArrayList<>(segmentLockSize);
        for (int i = 0; i < segmentLockSize; ++i) {
            tableGroupDigestList.add(tableGroupDigest + "#" + i);
        }
        return tableGroupDigestList;
    }

    public static class Counter {

        private volatile long value = 0;

        /**
         * 读操作，没有synchronized
         *
         * @return int
         */
        public long getValue() {
            return value;
        }

        /**
         * 写操作，synchronized 使其具备原子性
         */
        public synchronized void increment() {
            ++value;
        }
    }
}
