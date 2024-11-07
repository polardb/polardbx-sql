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

package com.alibaba.polardbx.executor.chunk;

import io.airlift.slice.Slice;

public abstract class ChunkUtil {

    public static int hashCode(byte[] arr, int start, int end) {
        if (arr == null) {
            return 0;
        }
        int result = 1;
        for (int i = start; i < end; ++i) {
            result = 31 * result + arr[i];
        }
        return result;
    }

    public static int hashCodeIgnoreCase(char[] arr, int start, int end) {
        if (arr == null) {
            return 0;
        }
        int result = 1;
        for (int i = start; i < end; ++i) {
            result = 31 * result + Character.toUpperCase(arr[i]);
        }
        return result;
    }

    public static int hashCode(Slice slice, int start, int end) {
        if (slice == null) {
            return 0;
        }
        int result = 1;
        for (int i = start; i < end; ++i) {
            result = 31 * result + slice.getByte(i);
        }
        return result;
    }
}
