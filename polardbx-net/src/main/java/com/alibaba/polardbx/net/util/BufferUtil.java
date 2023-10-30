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

package com.alibaba.polardbx.net.util;

import com.google.common.primitives.UnsignedLong;

import java.nio.ByteBuffer;

/**
 * @author xianmao.hexm 2010-9-3 下午02:29:44
 */
public class BufferUtil {

    public static final void writeUB3(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
    }

    public static final int getLength(long l) {
        /**
         * l should be compared as unsigned long
         * refer: com.google.common.primitives.UnsignedLong.compare
         */
        l = l ^ Long.MIN_VALUE;
        if (l < (251 ^ Long.MIN_VALUE)) {
            return 1;
        } else if (l < (0x10000L ^ Long.MIN_VALUE)) {
            return 3;
        } else if (l < (0x1000000L ^ Long.MIN_VALUE)) {
            return 4;
        } else {
            return 9;
        }
    }

    public static final int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

}
