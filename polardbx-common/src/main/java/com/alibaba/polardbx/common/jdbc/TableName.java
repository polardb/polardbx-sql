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

package com.alibaba.polardbx.common.jdbc;

import com.mysql.jdbc.StringUtils;

import java.sql.SQLException;

/**
 * @author moyi
 * @since 2021/08
 */
public class TableName {
    private String tableName;

    public TableName(String tableName) throws SQLException {
        this.checkName(tableName);
        this.tableName = tableName.trim();
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) throws SQLException {
        this.checkName(tableName);
        this.tableName = tableName.trim();
    }

    private void checkName(String tName) throws SQLException {
        if (StringUtils.isEmptyOrWhitespaceOnly(tName)) {
            throw new SQLException("tableName should not be empty", "S1009");
        } else {
            boolean needsHexEscape = false;
            String trimmedTableName = tName.trim();

            for (int i = 0; i < trimmedTableName.length(); ++i) {
                char c = trimmedTableName.charAt(i);
                switch (c) {
                case '\u0000':
                    needsHexEscape = true;
                    break;
                case '\n':
                    needsHexEscape = true;
                    break;
                case '\r':
                    needsHexEscape = true;
                    break;
                case '\u001a':
                    needsHexEscape = true;
                    break;
                case ' ':
                    needsHexEscape = true;
                    break;
                case '"':
                    needsHexEscape = true;
                    break;
                case '\'':
                    needsHexEscape = true;
                    break;
                case '\\':
                    needsHexEscape = true;
                }

                if (needsHexEscape) {
                    throw new SQLException("tableName format error: " + this.tableName, "S1009");
                }
            }

        }
    }

    @Override
    public String toString() {
        return getTableName();
    }
}

