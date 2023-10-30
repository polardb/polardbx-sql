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

package com.alibaba.polardbx.druid.sql.ast.statement;

public enum SqlDataAccess {
    // default value
    CONTAINS_SQL("CONTAINS SQL"),
    NO_SQL("NO SQL"),
    READS_SQL_DATA("READS SQL DATA"),
    MODIFIES_SQL_DATA("MODIFIES SQL DATA");

    private String content;

    SqlDataAccess(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return content;
    }
}
