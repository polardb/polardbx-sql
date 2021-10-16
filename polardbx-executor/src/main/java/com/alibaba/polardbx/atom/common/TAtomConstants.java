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

package com.alibaba.polardbx.atom.common;

import org.apache.commons.lang.StringUtils;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * TAtom数据源的常量设置类
 *
 * @author qihao
 */
public class TAtomConstants {
    public final static String DB_STATUS_R = "R";
    public final static String DB_STATUS_W = "W";
    public final static String DB_STATUS_RW = "RW";
    public final static String DB_STATUS_NA = "NA";

    public final static String DEFAULT_MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public final static String DEFAULT_DRUID_MYSQL_SORTER_CLASS =
        "com.alibaba.polardbx.common.jdbc.sorter.MySQLExceptionSorter";
    public final static String DRUID_MYSQL_INTEGRATION_SORTER_CLASS =
        "com.alibaba.polardbx.common.jdbc.sorter.MySQLExceptionSorter";
    public final static String DEFAULT_DRUID_MYSQL_VALID_CONNECTION_CHECKERCLASS =
        "com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker";
    public final static String DEFAULT_DRUID_MYSQL_VALIDATION_QUERY = "select 'x'";

    /**
     * dbName模板
     */
    private static final MessageFormat DB_NAME_FORMAT = new MessageFormat(
        "atom.dbkey.{0}^{1}^{2}");
    private static final String NULL_UNIT_NAME = "DEFAULT_UNIT";
    public static Map<String, String> DEFAULT_MYSQL_CONNECTION_PROPERTIES = new HashMap<String, String>(
        1);

    static {
        TAtomConstants.DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("characterEncoding", "gbk");
    }

    /**
     *
     */
    public static String getDbNameStr(String unitName, String appName, String dbkey) {
        if (StringUtils.isEmpty(unitName)) {
            return DB_NAME_FORMAT.format(new Object[] {NULL_UNIT_NAME, appName, dbkey});
        } else {
            return DB_NAME_FORMAT.format(new Object[] {appName, dbkey});
        }
    }
}
