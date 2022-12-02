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

import com.aliyun.oss.common.utils.CRC64;

public class CrcAccumulator {
    public final static byte SEPARATOR_TAG = (byte) 255;
    public final static byte NULL_TAG = (byte) 254;

    private CRC64 crc = new CRC64();
    private int lastHash;

    public void appendNull() {
        crc.update(NULL_TAG);
        crc.update(SEPARATOR_TAG);
        lastHash = NULL_TAG;
    }

    public void appendHash(int hash) {
        crc.update(hash);
        crc.update(SEPARATOR_TAG);
        lastHash = hash;
    }

    public void appendBytes(byte[] arr, int start, int end) {
        int hash = hashCode(arr, start, end);
        appendHash(hash);
    }

    public int lastHash() {
        return lastHash;
    }

    public long getResult() {
        return crc.getValue();
    }

    public void reset() {
        crc.reset();
        lastHash = NULL_TAG;
    }

    private static int hashCode(byte[] arr, int start, int end) {
        if (arr == null) {
            return 0;
        }
        int result = 1;
        for (int i = start; i < end; ++i) {
            result = 31 * result + arr[i];
        }
        return result;
    }
}
