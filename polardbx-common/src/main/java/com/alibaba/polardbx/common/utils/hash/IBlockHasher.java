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

import java.nio.charset.StandardCharsets;

/**
 * 按Block直接hash, 非流式
 * 操作均无状态
 * 避免内存拷贝
 */
public interface IBlockHasher {
    default HashResult128 hashByte(byte b) {
        return hashLong(b);
    }

    default HashResult128 hashShort(short s) {
        return hashLong(s);
    }

    default HashResult128 hashInt(int i) {
        return hashLong(i);
    }

    HashResult128 hashLong(long l);

    default HashResult128 hashDouble(double d) {
        return hashLong(Double.doubleToRawLongBits(d));
    }

    HashResult128 hashBytes(byte[] bytes);

    default HashResult128 hashString(String str) {
        // TODO charset
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        return hashBytes(data);
    }
}
