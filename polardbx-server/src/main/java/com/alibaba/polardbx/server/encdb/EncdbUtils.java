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

package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.server.executor.utils.MysqlDefs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author pangzhaoxing
 */
public class EncdbUtils {

    /**
     * @param sqlType java.sql.Types
     * @return jdbc protocol type
     */
    public static int sqlType2MysqlType(int sqlType, int scale) {
        int mysqlType;
        if (sqlType != DataType.UNDECIDED_SQL_TYPE) {
            mysqlType = MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(sqlType, scale));
        } else {
            mysqlType = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
        }
        return mysqlType;
    }

}
