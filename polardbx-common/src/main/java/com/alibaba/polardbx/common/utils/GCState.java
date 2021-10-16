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

package com.alibaba.polardbx.common.utils;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;


public class GCState {
    private static final List<String> youngGenCollectorNames = Arrays.asList(new String[]
            {
                    "Copy",
                    "ParNew",
                    "PS Scavenge",
                    "Garbage collection optimized for short pausetimes Young Collector",
                    "Garbage collection optimized for throughput Young Collector",
                    "Garbage collection optimized for deterministic pausetimes Young Collector"
            });

    private static final List<String> OldGenCollectorNames = Arrays.asList(new String[]
            {

                "MarkSweepCompact",

                "PS MarkSweep",

                "ConcurrentMarkSweep",

                "Garbage collection optimized for short pausetimes Old Collector",

                "Garbage collection optimized for throughput Old Collector",

                "Garbage collection optimized for deterministic pausetimes Old Collector"
            });


    public static long getYoungGenCollectionCount(){
        long youngGCCounts = 0L;
        List<GarbageCollectorMXBean> list = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gmx : list) {
            if(youngGenCollectorNames.contains(gmx.getName())){
                youngGCCounts+=gmx.getCollectionCount();
            }
        }
        return youngGCCounts;
    }

    public static long getOldGenCollectionCount(){
        long fullGCCounts = 0L;
        List<GarbageCollectorMXBean> list = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gmx : list) {
            if(OldGenCollectorNames.contains(gmx.getName())){
                fullGCCounts+=gmx.getCollectionCount();
            }
        }
        return fullGCCounts;
    }

    public static void main(String[] args) {
        System.out.println(getOldGenCollectionCount());
        System.out.println(getYoungGenCollectionCount());
    }

    public static Object getOldGenCollectionTime() {
        long fullGCTime = 0L;
        List<GarbageCollectorMXBean> list = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gmx : list) {
            if(OldGenCollectorNames.contains(gmx.getName())){
                fullGCTime+=gmx.getCollectionTime();

            }
        }
        return fullGCTime;
    }
}