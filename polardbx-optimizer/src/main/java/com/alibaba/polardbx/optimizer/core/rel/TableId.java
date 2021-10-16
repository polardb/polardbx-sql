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

package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * 表名完整定义
 *
 * @author biliang.wbl
 */
public class TableId implements Serializable {

    private final String dbName;
    private final String tableName;

    public TableId(String dbName, String tableName) {
        super();
        Preconditions.checkArgument(StringUtils.isNotEmpty(tableName));
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public final String getDbName() {
        return dbName;
    }

    public final String getTableName() {
        return tableName;
    }

    @Override
    public final int hashCode() {
        return (dbName.toLowerCase().hashCode() << 8) + tableName.toLowerCase().hashCode();
    }

    public ImmutableList<String> list() {
        if (StringUtils.isEmpty(dbName)) {
            return ImmutableList.of(tableName);
        } else {
            return ImmutableList.of(dbName, tableName);
        }
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(dbName)) {
            sb.append(dbName).append(".");
        }

        return sb.append(tableName).toString();
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof TableId) {
            TableId tn = (TableId) obj;
            return StringUtils.equalsIgnoreCase(tn.dbName, dbName)
                && StringUtils.equalsIgnoreCase(tn.tableName, tableName);
        }
        return false;
    }
}
