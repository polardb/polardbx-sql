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

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.charset.StandardCharsets;

@NotThreadSafe
/**
 * 流式hash, 最后获取hash结果
 * TODO 流式Buffer封装 以及 模式混用的保护
 */
public interface IStreamingHasher {

    default IStreamingHasher putByte(byte b) {
        return putLong(b);
    }

    default IStreamingHasher putShort(short s) {
        return putLong(s);
    }

    default IStreamingHasher putInt(int i) {
        return putLong(i);
    }

    IStreamingHasher putLong(long l);

    default IStreamingHasher putDouble(double d) {
        return putLong(Double.doubleToRawLongBits(d));
    }

    IStreamingHasher putBytes(byte[] bytes);

    default IStreamingHasher putString(String str) {
        return putBytes(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 获取最终结果
     */
    HashResult128 hash();

    void reset();
}
