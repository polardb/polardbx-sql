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

package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IdGenerator {

    protected final static Logger logger = LoggerFactory.getLogger(IdGenerator.class);

    private static final Set idGenerators = Collections.synchronizedSet(new HashSet<IdGenerator>());

    public static final int MAX_NUM_OF_IDS_PER_RANGE = 4096;

    private static final long TWEPOCH = 1303895660503L;

    private static final long WORKER_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    private static final long MAX_WORKER_ID = -1L ^ -1L << WORKER_ID_BITS;

    private static final long TIMESTAMP_MASK = -1L << TIMESTAMP_LEFT_SHIFT;
    private static final long SEQUENCE_MASK = -1L ^ -1L << SEQUENCE_BITS;
    private static final long WORKER_ID_MASK = -1L ^ TIMESTAMP_MASK ^ SEQUENCE_MASK;

    private volatile long workerId;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public static IdGenerator getIdGenerator() {
        IdGenerator idGenerator = new IdGenerator(TddlNode.getNodeId());

        idGenerators.add(idGenerator);
        return idGenerator;
    }

    public static IdGenerator getDefaultIdGenerator() {
        return new IdGenerator(TddlNode.DEFAULT_SERVER_NODE_ID);
    }

    public static void rebindAll() {
        synchronized (idGenerators) {
            Iterator<IdGenerator> iter = idGenerators.iterator();
            while (iter.hasNext()) {
                IdGenerator idGenerator = iter.next();
                idGenerator.rebind(TddlNode.getNodeId());
            }
        }
    }

    public static void remove(IdGenerator idGenerator) {
        if (idGenerator != null) {
            idGenerators.remove(idGenerator);
        }
    }

    private IdGenerator(long workerId) {
        super();
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        this.workerId = workerId;
    }

    public void rebind(long workerId) {
        if (this.workerId != workerId) {
            this.workerId = workerId;
        }
    }

    public synchronized long nextId() {
        long timestamp = 0;
        int tryTimes = 0;
        do {
            tryTimes++;
            timestamp = this.timeGen();

            if (this.lastTimestamp == timestamp) {
                this.sequence = this.sequence + 1 & SEQUENCE_MASK;
                if (this.sequence == 0) {
                    timestamp = this.tilNextMillis(this.lastTimestamp);
                }
            } else {
                this.sequence = 0;
            }
        } while (timestamp < this.lastTimestamp);

        if (tryTimes > 10) {
            if (logger.isDebugEnabled()) {
                logger.warn("Clock moved backwards " + tryTimes
                    + " times! It may be ugly if this warning appear too much times.");
            }
        }

        this.lastTimestamp = timestamp;

        return assembleId(timestamp, this.workerId, this.sequence);
    }

    public synchronized long nextId(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("Size must be greater than 0.");
        }
        if (size == 1) {
            return nextId();
        }

        boolean needMoreRanges = true;
        long timestamp = timeGen();

        if (this.lastTimestamp == timestamp) {
            int numOfAssignableIdsInFirstRange = MAX_NUM_OF_IDS_PER_RANGE - ((int) this.sequence) - 1;
            if (numOfAssignableIdsInFirstRange < 0) {

                numOfAssignableIdsInFirstRange = 0;
            }
            if (numOfAssignableIdsInFirstRange >= size) {

                this.sequence += size;
                needMoreRanges = false;
            } else {
                size -= numOfAssignableIdsInFirstRange;
                this.sequence = 0;

                timestamp = tilNextMillis(this.lastTimestamp);
            }
        } else {
            this.sequence = 0;
            while (timestamp < this.lastTimestamp) {
                timestamp = timeGen();
            }
        }

        if (needMoreRanges) {

            long numOfFullRanges = size / MAX_NUM_OF_IDS_PER_RANGE;

            long numOfIdsInLastRange = size % MAX_NUM_OF_IDS_PER_RANGE;

            if (numOfIdsInLastRange == 0) {
                numOfFullRanges--;
                numOfIdsInLastRange = MAX_NUM_OF_IDS_PER_RANGE;
            }

            if (numOfFullRanges > 0) {

                timestamp += numOfFullRanges;

                while (this.lastTimestamp <= timestamp) {
                    this.lastTimestamp = this.timeGen();
                }
            }

            this.sequence += numOfIdsInLastRange - 1;
        }

        this.lastTimestamp = timestamp;

        return assembleId(timestamp, this.workerId, this.sequence);
    }

    public static long assembleId(long timestamp, long workerId, long sequence) {
        return ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT) | (workerId << WORKER_ID_SHIFT) | (sequence);
    }

    public static String extractId(long id) {
        return extractTimestamp(id) + "," + extractWorkerId(id) + "," + extractSequence(id);
    }

    public static long extractTimestamp(long id) {
        return ((id & TIMESTAMP_MASK) >>> TIMESTAMP_LEFT_SHIFT) + TWEPOCH;
    }

    public static long extractWorkerId(long id) {
        return (id & WORKER_ID_MASK) >>> WORKER_ID_SHIFT;
    }

    public static long extractSequence(long id) {
        return id & SEQUENCE_MASK;
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            File idList = new File(args[0]);
            if (idList.exists()) {
                BufferedReader br = null;
                try {
                    br = new BufferedReader(new FileReader(idList));
                    String id;
                    while ((id = br.readLine()) != null) {
                        System.out.println(extractId(Long.valueOf(id)));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (br != null) {
                        try {
                            br.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } else {
                System.out.println(extractId(Long.valueOf(args[0])));
            }
        }
    }

}
