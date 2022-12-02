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

package com.alibaba.polardbx.executor.mdl;

import com.google.common.base.Preconditions;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.Objects;

/**
 * MDL 锁对象的标识，是一个三元组 namespace + dbName + tableName
 *
 * @author chenmo.cm
 */
public final class MdlKey {

    /**
     * 加锁对象类型
     */
    final MdlNamespace namespace;
    final String dbName;
    final String tableName;

    public MdlKey(@NotNull MdlNamespace namespace, @NotNull String dbName, @NotNull String tableName) {
        this.namespace = namespace;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    /**
     * 默认不区分表名大小写
     */
    public static MdlKey getTableKeyWithLowerTableName(@NotNull String dbName, @NotNull String tableName) {
        Preconditions.checkNotNull(dbName);
        Preconditions.checkNotNull(tableName);
        return new MdlKey(MdlNamespace.TABLE, dbName, tableName.toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MdlKey)) {
            return false;
        }
        MdlKey that = (MdlKey) o;
        return namespace == that.namespace && Objects.equals(dbName, that.dbName)
            && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, dbName, tableName);
    }

    public MdlNamespace getNamespace() {
        return namespace;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "MdlKey{" + "namespace=" + namespace + ", dbName='" + dbName + '\'' + ", tableName='" + tableName + '\''
            + '}';
    }
}
