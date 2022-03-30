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

package com.alibaba.polardbx.common.utils.bloomfilter;

import com.alibaba.polardbx.common.utils.hash.HashMethodInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BloomFilterInfo {
    private static final int LONG_BYTES_SIZE = 8;
    private final Integer id;
    private final int hashFuncNum;
    private final long[] data;
    private final HashMethodInfo hashMethodInfo;
    private byte[] bytesData;

    @JsonCreator
    public BloomFilterInfo(
        @JsonProperty("id") Integer id,
        @JsonProperty("data") long[] data,
        @JsonProperty("hashFuncNum") int hashFuncNum,
        @JsonProperty("hashMethodInfo") HashMethodInfo hashMethodInfo) {
        this.id = id;
        this.data = data;
        this.hashFuncNum = hashFuncNum;
        this.hashMethodInfo = hashMethodInfo;
    }

    @JsonProperty
    public Integer getId() {
        return id;
    }

    @JsonProperty
    public long[] getData() {
        return data;
    }

    @JsonProperty
    public int getHashFuncNum() {
        return hashFuncNum;
    }

    public int getDataLenInBits() {
        return data.length * 64;
    }

    public byte[] getBytesData() {
        synchronized (this) {
            if (bytesData == null) {
                bytesData = toBytes(data);
            }

            return bytesData;
        }
    }

    @JsonProperty
    public HashMethodInfo getHashMethodInfo() {
        return hashMethodInfo;
    }

    public synchronized void mergeBloomFilter(BloomFilterInfo other) {
        Preconditions.checkArgument(this.id.equals(other.id), "Bloom filter info id should be same!");
        Preconditions.checkArgument(this.hashMethodInfo.equals(other.hashMethodInfo),
            "Bloom filter hash method info should be same!");
        Preconditions.checkArgument(this.hashFuncNum == other.hashFuncNum,
            "Bloom filter info hash function num should be same!");
        Preconditions.checkArgument(data.length == other.getData().length, "Bit array length must match!");

        long[] sourceData = other.data;

        for (int i = 0; i < data.length; i++) {
            data[i] |= sourceData[i];
        }

        this.bytesData = null;
    }

    public BloomFilter toBloomFilter() {
        return BloomFilter.createWithData(hashMethodInfo, hashFuncNum, data);
    }

    @Override
    public String toString() {
        return "BloomFilterInfo{" +
            "id=" + id +
            ", hashFuncNum=" + hashFuncNum +
            '}';
    }

    private static byte[] toBytes(long[] data) {
        byte[] buffer = new byte[data.length * LONG_BYTES_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        for (long d : data) {
            bb.putLong(d);
        }
        return buffer;
    }
}
