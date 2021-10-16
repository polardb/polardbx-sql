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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;

import java.util.Comparator;

import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;

public class SensitiveStringType extends AbstractUTF8StringType implements DataType<String> {
    public static final Comparator<String> CASE_SENSITIVE_ORDER = new CaseSensitiveComparator();

    @Override
    public MySQLStandardFieldType fieldType() {
        return MYSQL_TYPE_VAR_STRING;
    }

    private static class CaseSensitiveComparator implements Comparator<String>, java.io.Serializable {

        private static final long serialVersionUID = 8575799808333029326L;

        @Override
        public int compare(String s1, String s2) {
            int n1 = s1.length();
            int n2 = s2.length();
            int min = Math.min(n1, n2);
            for (int i = 0; i < min; i++) {
                char c1 = s1.charAt(i);
                char c2 = s2.charAt(i);
                if (c1 != c2) {
                    return c1 - c2;
                }
            }

            return n1 - n2;
        }
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        String no1 = convertFrom(o1);
        String no2 = convertFrom(o2);

        if (no1 == null) {
            return -1;
        }

        if (no2 == null) {
            return 1;
        }
        return CASE_SENSITIVE_ORDER.compare(no1, no2);
    }

}
