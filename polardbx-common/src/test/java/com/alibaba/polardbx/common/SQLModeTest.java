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

package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.utils.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SQLModeTest {
    private static final Random R = new Random();

    String ANSI = "ANSI";
    String DB_2 = "DB2";
    String MAXDB = "MAXDB";
    String MSSQL = "MSSQL";
    String ORACLE = "ORACLE";
    String POSTGRESQL = "POSTGRESQL";
    String TRADITIONAL = "TRADITIONAL";
    String[] theCombined = {ANSI, DB_2, MAXDB, MSSQL, ORACLE, POSTGRESQL, TRADITIONAL};
    List<String> allSqlMode;

    public SQLModeTest() {
        allSqlMode = Arrays.stream(SQLMode.values()).map(Enum::name).collect(
            Collectors.toList());
        allSqlMode.addAll(Arrays.asList(theCombined));
    }

    public String randomSqlModeStr() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < allSqlMode.size(); i++) {
            if (R.nextInt() % 2 == 0) {
                stringBuilder.append(allSqlMode.get(i));
            }
        }
        return stringBuilder.toString();
    }

    @Test
    public void test() {
        IntStream.range(0, 1 << 10).forEach(
            i -> {
                // convert from sql mode to flag
                String sqlModeStr = randomSqlModeStr();
                long flag = SQLMode.convertToFlag(sqlModeStr);

                // check all sql mode
                SQLMode.convertFromFlag(flag).forEach(
                    sqlMode -> Assert.assertTrue((flag & sqlMode.getModeFlag()) != 0)
                );
            }
        );
    }

}
