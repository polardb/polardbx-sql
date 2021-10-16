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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import junit.framework.TestCase;

public class NameTest extends TestCase {
    static final String str = "BEFORE,BINARY,BLOB,BOTH,CALL,CASCADE,CHANGE,CHAR,CHARACTER,COLLATE,CONDITION,"
            + "CONNECTION,CONSTRAINT,CONTINUE,CONVERT,CROSS,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DATABASES,"
            + "DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DEC,DECIMAL,DECLARE,DEFAULT,DELAYED,DETERMINISTIC,DISTINCTROW,DIV,DOUBLE,"
            + "DUAL,EACH,ELSEIF,ENCLOSED,ESCAPED,EXIT,FETCH,FLOAT,FOR,FORCE,FOREIGN,FUNCTION,GOTO,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,"
            + "IF,INFILE,INT,INTEGER,ITERATE,KILL,LABEL,LEADING,LEAVE,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LOOP,MATCH,MINUTE_MICROSECOND,"
            + "MINUTE_SECOND,MOD,MODIFIES,OPTIMIZE,OPTION,OUT,OUTFILE,PRECISION,RANGE,READ,REAL,REFERENCES,REGEXP,RELEASE,RENAME,REPEAT,"
            + "REQUIRE,RESTRICT,RETURN,RLIKE,SCHEMA,SECOND,MICROSECOND,SEPARATOR,SMALLINT,SPATIAL,SPECIFIC,SQL,TINYINT,TINYBLOB,UNLOCK,"
            + "UNSIGNED,USE,VARCHAR,WHILE,WRITE,XOR,YEAR_MONTH";

    String[] names;
    protected void setUp() throws Exception {
        names = str.split(",");
    }

    public void test_for_names() throws Exception {
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            String createTable = "create table " + name + " (fid varchar(50));";
            try {
                SQLUtils.parseSingleMysqlStatement(createTable);
            } catch (ParserException ex) {
                // skip
                System.out.println("error name : " + name);
            }

        }

    }
}
