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

package com.alibaba.polardbx.optimizer.statis;

/**
 * @author mengshi.sunmengshi 2016年3月10日 下午4:53:29
 * @since 5.1.0
 */
public final class SQLRecord implements Comparable<SQLRecord> {

    // DRDS事务ID
    public static final String TX_ID = "TX_ID";

    // 前端连接ID
    public static final String CONN_ID = "CONNECTION_ID";

    // 一个事务内的SQL的ID(一般是一个事务内的第几条SQL)
    public static final String SQL_ID = "SQL_ID";

    // 客户端IP
    public static final String CLIENT_IP = "CLIENT_IP";

    // 全局唯一的每个发往DRDS的SQL的追踪ID
    public static final String TRACE_ID = "TRACE_ID";

    public boolean physical = false;
    public String user;
    public String host;
    public String port;
    public String schema;
    public String statement;
    public long startTime;
    public long executeTime;
    public long getLockConnectionTime;                        // 链接等待时间
    public long createConnectionTime;                         // 链接创建时间
    public long sqlTime;
    public String dataNode;
    public String dbKey;
    public long affectRow = -1;
    public String traceId;
    public long id = 0;

    @Override
    public int compareTo(SQLRecord o) {
        return (int) (executeTime - o.executeTime);
    }

    @Override
    public String toString() {
        return this.statement;
    }
}
