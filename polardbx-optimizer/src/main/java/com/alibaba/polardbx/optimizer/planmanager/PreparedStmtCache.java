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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;

public class PreparedStmtCache {
    /**
     * 事务控制语句共享同一个缓存对象
     */
    public static final PreparedStmtCache BEGIN_PREPARE_STMT_CACHE = new PreparedStmtCache();
    public static final PreparedStmtCache COMMIT_PREPARE_STMT_CACHE = new PreparedStmtCache();
    public static final PreparedStmtCache ROLLBACK_PREPARE_STMT_CACHE = new PreparedStmtCache();

    private final Statement stmt;
    private SqlParameterized sqlParameterized;
    private SqlType sqlType;

    private int charsetIndex;
    private String javaCharset;
    private byte[] catalog;
    private FieldPacket[] fieldPackets;

    private PreparedStmtCache() {
        this.stmt = null;
    }

    public PreparedStmtCache(Statement stmt) {
        this.stmt = stmt;
    }

    public Statement getStmt() {
        return stmt;
    }

    public SqlParameterized getSqlParameterized() {
        return sqlParameterized;
    }

    public void setSqlParameterized(SqlParameterized sqlParameterized) {
        this.sqlParameterized = sqlParameterized;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    public String getJavaCharset() {
        return javaCharset;
    }

    public void setJavaCharset(String javaCharset) {
        this.javaCharset = javaCharset;
    }

    public byte[] getCatalog() {
        return catalog;
    }

    public void setCatalog(byte[] catalog) {
        this.catalog = catalog;
    }

    public FieldPacket[] getFieldPackets() {
        return fieldPackets;
    }

    public void setFieldPackets(FieldPacket[] fieldPackets) {
        this.fieldPackets = fieldPackets;
    }
}