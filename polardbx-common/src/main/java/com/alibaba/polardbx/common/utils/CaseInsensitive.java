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

import java.util.Comparator;

public class CaseInsensitive {

    public static final Comparator<String> CASE_INSENSITIVE_ORDER = new CaseInsensitiveComparator();

    private static class CaseInsensitiveComparator implements Comparator<String>, java.io.Serializable {

        private static final long serialVersionUID = 8575799808933029326L;

        @Override
        public int compare(String s1, String s2) {
            if (s1 == s2) {
                return 0;
            }
            int n1 = s1.length();
            int n2 = s2.length();
            for (int i1 = 0, i2 = 0; i1 < n1 && i2 < n2; i1++, i2++) {
                char c1 = s1.charAt(i1);
                char c2 = s2.charAt(i2);
                if (c1 != c2) {
                    c1 = Character.toUpperCase(c1);
                    c2 = Character.toUpperCase(c2);
                    if (c1 != c2) {

                        return c1 - c2;
                    }
                }
            }
            return n1 - n2;
        }
    }

    public static int compareToIgnoreCase(String s1, String s2) {
        return CASE_INSENSITIVE_ORDER.compare(s1, s2);
    }
}
