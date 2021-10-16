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

package com.alibaba.polardbx.druid.benchmark;

import com.alibaba.polardbx.druid.util.JdbcUtils;
import com.alibaba.polardbx.druid.util.Utils;

import java.io.InputStream;

public class TPCDS {
    private static String ddl;
    private static String[] QUERIES = new String[99];

    static {
        for (int i = 1; i <= 99; ++i) {
            String num = (i < 10 ? "0" : "") + i;
            String path = "tpcds/query" + num + ".sql";
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
            String sql = Utils.read(is);
            QUERIES[i-1] = sql;
            JdbcUtils.close(is);
        }
        {
            String path = "tpcds/create_tables.sql";
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
            ddl = Utils.read(is);
            JdbcUtils.close(is);
        }
    }

    public static String getQuery(int index) {
        return QUERIES[index - 1];
    }

    public static String getDDL() {
        return ddl;
    }
}
