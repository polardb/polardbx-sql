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

package com.alibaba.polardbx.qatest.constant;

public class ConfigConstant {

    public static final String RESOURCE_PATH = ConfigConstant.class.getClassLoader().getResource(".").getPath();
    public static final String CONN_CONFIG = RESOURCE_PATH + "qatest.properties";

    public static final String SKIP_INIT_MYSQL = "skipInitMysql";
    public static final String MYSQL_USER = "mysqlUserName";
    public static final String MYSQL_PASSWORD = "mysqlPassword";
    public static final String MYSQL_PORT = "mysqlPort";
    public static final String MYSQL_ADDRESS = "mysqlAddr";

    public static final String MYSQL_USER_SECOND = "mysqlUserNameSecond";
    public static final String MYSQL_PASSWORD_SECOND = "mysqlPasswordSecond";
    public static final String MYSQL_PORT_SECOND = "mysqlPortSecond";
    public static final String MYSQL_ADDRESS_SECOND = "mysqlAddrSecond";

    public static final String POLARDBX_USER = "polardbxUserName";
    public static final String POLARDBX_PASSWORD = "polardbxPassword";
    public static final String POLARDBX_PORT = "polardbxPort";
    public static final String POLARDBX_ADDRESS = "polardbxAddr";

    public static final String META_DB = "metaDbName";
    public static final String META_USER = "metaDbUser";
    public static final String META_PASSWORD = "metaDbPasswd";
    public static final String META_PORT = "metaPort";
    public static final String META_ADDRESS = "metaDbAddr";

    public static final String URL_PATTERN = "jdbc:mysql://%s:%s?";
    public static final String URL_PATTERN_WITH_DB = "jdbc:mysql://%s:%s/%s?";

    public static final String CDC_CHECK_DB_BLACKLIST = "cdcCheckDbBlackList";
    public static final String CDC_CHECK_TABLE_BLACKLIST = "cdcCheckTableBlackList";
    public static final String CDC_CHECK_DB_ALLOWLIST = "cdcCheckDbAllowList";
    public static final String CDC_CHECK_TABLE_ALLOWLIST = "cdcCheckTableAllowList";
}
