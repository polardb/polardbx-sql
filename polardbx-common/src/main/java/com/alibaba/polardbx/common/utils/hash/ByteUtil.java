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

package com.alibaba.polardbx.common.utils.hash;

import java.nio.ByteOrder;

public class ByteUtil {
    public static final ByteOrder PLATFORM_ENDIAN = ByteOrder.nativeOrder();

    /**
     * 从 start 处取8字节转为long
     */
    public static long getBlock64Uncheck(byte[] bytes, int start) {
        if (ByteOrder.BIG_ENDIAN == PLATFORM_ENDIAN) {
            return (bytes[start] & 0xFFL) << 56
                | (bytes[start + 1] & 0xFFL) << 48
                | (bytes[start + 2] & 0xFFL) << 40
                | (bytes[start + 3] & 0xFFL) << 32
                | (bytes[start + 4] & 0xFFL) << 24
                | (bytes[start + 5] & 0xFFL) << 16
                | (bytes[start + 6] & 0xFFL) << 8
                | (bytes[start + 7] & 0xFFL);
        } else {
            return (bytes[start + 7] & 0xFFL) << 56
                | (bytes[start + 6] & 0xFFL) << 48
                | (bytes[start + 5] & 0xFFL) << 40
                | (bytes[start + 4] & 0xFFL) << 32
                | (bytes[start + 3] & 0xFFL) << 24
                | (bytes[start + 2] & 0xFFL) << 16
                | (bytes[start + 1] & 0xFFL) << 8
                | (bytes[start] & 0xFFL);
        }
    }

    /**
     * 从 start 处取4字节转为int
     */
    public static int getBlock32Uncheck(byte[] bytes, int start) {
        if (ByteOrder.BIG_ENDIAN == PLATFORM_ENDIAN) {
            return (bytes[start + 4] & 0xFF) << 24
                | (bytes[start + 5] & 0xFF) << 16
                | (bytes[start + 6] & 0xFF) << 8
                | (bytes[start + 7] & 0xFF);
        } else {
            return (bytes[start + 3] & 0xFF) << 24
                | (bytes[start + 2] & 0xFF) << 16
                | (bytes[start + 1] & 0xFF) << 8
                | (bytes[start] & 0xFF);
        }
    }

    public static void putInt64Uncheck(byte[] dest, int start, long l) {
        if (ByteOrder.BIG_ENDIAN == PLATFORM_ENDIAN) {
            dest[start + 7] = (byte) (l & 0xff);
            dest[start + 6] = (byte) (l >> 8 & 0xff);
            dest[start + 5] = (byte) (l >> 16 & 0xff);
            dest[start + 4] = (byte) (l >> 24 & 0xff);
            dest[start + 3] = (byte) (l >> 32 & 0xff);
            dest[start + 2] = (byte) (l >> 40 & 0xff);
            dest[start + 1] = (byte) (l >> 48 & 0xff);
            dest[start] = (byte) (l >> 56 & 0xff);
        } else {
            dest[start] = (byte) (l & 0xff);
            dest[start + 1] = (byte) (l >> 8 & 0xff);
            dest[start + 2] = (byte) (l >> 16 & 0xff);
            dest[start + 3] = (byte) (l >> 24 & 0xff);
            dest[start + 4] = (byte) (l >> 32 & 0xff);
            dest[start + 5] = (byte) (l >> 40 & 0xff);
            dest[start + 6] = (byte) (l >> 48 & 0xff);
            dest[start + 7] = (byte) (l >> 56 & 0xff);
        }
    }
}
