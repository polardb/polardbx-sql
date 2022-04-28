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

public class ExecUtils {
    public static boolean arrayEquals(byte[] arr1, int pos1, int len1, byte[] arr2, int pos2, int len2) {
        if (len1 != len2) {
            return false;
        }
        for (int i = 0; i < len1; i++) {
            if (arr1[pos1 + i] != arr2[pos2 + i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean arrayEquals(
        char[] arr1, int pos1, int len1, char[] arr2, int pos2, int len2, boolean ingoreCase) {
        if (len1 != len2) {
            return false;
        }
        for (int i = 0; i < len1; i++) {
            if (ingoreCase) {
                if (Character.toUpperCase(arr1[pos1 + i]) != Character.toUpperCase(arr2[pos2 + i])) {
                    return false;
                }
            } else {
                if (arr1[pos1 + i] != arr2[pos2 + i]) {
                    return false;
                }
            }
        }
        return true;
    }
}
