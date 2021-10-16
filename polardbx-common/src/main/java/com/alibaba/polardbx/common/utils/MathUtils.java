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

public class MathUtils {

    public static int ceilDiv(int x, int y) {
        return -Math.floorDiv(-x, y);
    }

    public static long ceilDiv(long x, long y) {
        return -Math.floorDiv(-x, y);
    }

    public static int ceilMod(int x, int y) {
        return x - ceilDiv(x, y) * y;
    }

    public static long ceilMod(long x, long y) {
        return x - ceilDiv(x, y) * y;
    }

}
