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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;

import java.sql.Connection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 用来描述一个逻辑表由哪些key-val组成的 <br/>
 * 屏蔽掉不同的schema存储，存储可能会是本地,diamond或zk schema
 *
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public interface SchemaManager extends Lifecycle {

    // tddl系统表
    public static final String DUAL = "dual";

    public TableMeta getTable(String tableName);

    public default TableMeta getTableWithNull(String tableName) {
        if (tableName == null) {
            return null;
        }
        try {
            return this.getTable(tableName.toLowerCase());
        } catch (TableNotFoundException e) {
            return null;
        } catch (Throwable t) {
            if (t.getMessage() != null && t.getMessage().contains("doesn't exist")) {
                return null;
            }

            throw t;
        }
    }

    public void putTable(String tableName, TableMeta tableMeta);

    public void reload(String tableName);

    public void invalidate(String tableName);

    public void invalidateAll();

    public GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet);

    default Set<String> guessGsi(String unwrappedName) {
        return Collections.emptySet();
    }

    default TableMeta getTableMetaFromConnection(String tableName, Connection conn) {
        throw new UnsupportedOperationException();
    }

    public String getSchemaName();

    public default void expire() {
    }

    public default boolean isExpired() {
        return false;
    }

    default Map<String, TableMeta> getCache() {
        return new HashMap<>();
    }

    default TddlRuleManager getTddlRuleManager() {
        return null;
    }

    default void toNewVersionInTrx(List<String> tableNameList,
                                   boolean preemtive, long initWait, long interval, TimeUnit timeUnit,
                                   boolean allowTwoVersion) {
        throw new AssertionError("NOT SUPPORTED");
    }

    default void toNewVersionInTrx(List<String> tableNameList, boolean allowTwoVersion) {
        throw new AssertionError("NOT SUPPORTED");
    }

    default void toNewVersionForTableGroup(String tableName, boolean allowTwoVersion) {
        throw new AssertionError("NOT SUPPORTED");
    }
}
