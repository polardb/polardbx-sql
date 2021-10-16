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

package com.alibaba.polardbx.optimizer.partition.datatype;

public abstract class AbstractNumericPartitionField extends AbstractPartitionField {
    public static final long INT_64_MAX = 0x7FFFFFFFFFFFFFFFL;
    public static final long INT_64_MIN = -0x7FFFFFFFFFFFFFFFL - 1;
    public static final long UNSIGNED_INT_64_MAX = 0xFFFFFFFFFFFFFFFFL;
    public static final long UNSIGNED_INT_64_MIN = 0L;

    public static final int INT_32_MAX = 0x7FFFFFFF;
    public static final int INT_32_MIN = ~0x7FFFFFFF;
    public static final int UNSIGNED_INT_32_MAX = 0xFFFFFFFF;
    public static final int UNSIGNED_INT_32_MIN = 0;

    public static final int INT_24_MAX = 0x007FFFFF;
    public static final int INT_24_MIN = ~0x007FFFFF;
    public static final int UNSIGNED_INT_24_MAX = 0x00FFFFFF;
    public static final int UNSIGNED_INT_24_MIN = 0;

    public static final int INT_16_MAX = 0x7FFF;
    public static final int INT_16_MIN = ~0x7FFF;
    public static final int UNSIGNED_INT_16_MAX = 0xFFFF;
    public static final int UNSIGNED_INT_16_MIN = 0;

    public static final int INT_8_MAX = 0x7F;
    public static final int INT_8_MIN = ~0x7F;
    public static final int UNSIGNED_INT_8_MAX = 0xFF;
    public static final int UNSIGNED_INT_8_MIN = 0;

    protected boolean isUnsigned;
}
