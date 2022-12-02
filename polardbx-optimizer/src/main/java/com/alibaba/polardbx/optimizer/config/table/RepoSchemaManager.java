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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.EnumSet;

/**
 * 基于repo的{@linkplain SchemaManager}的委托实现
 *
 * @author jianghang 2013-11-19 下午4:53:08
 * @since 5.0.0
 */
public class RepoSchemaManager extends AbstractLifecycle implements SchemaManager {

    private RepoSchemaManager delegate;
    private boolean isDelegate;
    private Group group;

    protected TddlRuleManager rule = null;

    public static final String FETCH_TABLE_ERROR = "FETCH_TABLE_ERROR";
    protected String schemaName;

    public RepoSchemaManager(String schema) {
        this.schemaName = schema;
    }

    public void setRule(TddlRuleManager rule) {
        this.rule = rule;
    }

    @Override
    public TddlRuleManager getTddlRuleManager() {
        return this.rule;
    }

    public static class LogicalAndActualTableName {

        public String logicalTableName;
        public String actualTableName;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((logicalTableName == null) ? 0 : logicalTableName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            LogicalAndActualTableName other = (LogicalAndActualTableName) obj;
            if (logicalTableName == null) {
                if (other.logicalTableName != null) {
                    return false;
                }
            } else if (!logicalTableName.equals(other.logicalTableName)) {
                return false;
            }
            return true;
        }
    }

    @Override
    protected void doInit() {
        if (!isDelegate) {
            delegate = ExtensionLoader.load(RepoSchemaManager.class, group.getType().name());
            delegate.setGroup(group);
            delegate.setDelegate(true);
            delegate.setRule(rule);
            delegate.init();
        }
    }

    @Override
    public final TableMeta getTable(String tableName) {
        return getTable(tableName, null);
    }

    public final TableMeta getTable(String logicalTableName, String actualTableName) {
        if (logicalTableName.equalsIgnoreCase(DUAL)) {
            return buildDualTable();
        }

        if (ConfigDataMode.isFastMock()) {
            actualTableName = logicalTableName;
        }

        TableMeta meta = getTable0(logicalTableName, actualTableName);

        if (meta == null) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, logicalTableName);
        } else {
            meta.setSchemaName(this.getTddlRuleManager().getTddlRule().getSchemaName());
            return meta;
        }
    }

    @Override
    public final void putTable(String tableName, TableMeta tableMeta) {
        if (null != tableName) {
            tableName = tableName.toLowerCase();
        }
        delegate.putTable(tableName, tableMeta);
    }

    /**
     * 需要各Repo来实现
     */
    protected TableMeta getTable0(String logicalTableName, String actualTableName) {
        throw new NotSupportException();
    }

    @Override
    protected void doDestroy() {
        if (!isDelegate) {
            delegate.destroy();
        }
    }

    protected TableMeta buildDualTable() {
        IndexMeta index = new IndexMeta(SchemaManager.DUAL,
            new ArrayList<ColumnMeta>(),
            new ArrayList<ColumnMeta>(),
            IndexType.NONE,
            Relationship.NONE,
            false,
            true,
            true,
            "");

        return new TableMeta(getSchemaName(), DUAL, new ArrayList<ColumnMeta>(), index, new ArrayList<IndexMeta>(),
            true,
            TableStatus.PUBLIC, 0, 0);
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public Group getGroup() {
        return group;
    }

    public void setDelegate(boolean isDelegate) {
        this.isDelegate = isDelegate;
    }

    @Override
    public void reload(String tableName) {
        if (this.delegate != null) {
            if (null != tableName) {
                tableName = tableName.toLowerCase();
            }
            this.delegate.reload(tableName);
        }
    }

    @Override
    public void invalidate(String tableName) {
        if (this.delegate != null) {
            if (null != tableName) {
                tableName = tableName.toLowerCase();
            }
            this.delegate.invalidate(tableName);
        }
    }

    @Override
    public void invalidateAll() {
        if (this.delegate != null) {
            this.delegate.invalidateAll();
        }
    }

    public GsiMetaManager getGsiMetaManager() {
        return delegate.getGsiMetaManager();
    }

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        final GsiMetaManager gsiMetaManager = getGsiMetaManager();
        return null == gsiMetaManager ? GsiMetaManager.GsiMetaBean.empty() : gsiMetaManager.getTableAndIndexMeta(
            primaryOrIndexTableName, statusSet);
    }

    @Override
    public TableMeta getTableMetaFromConnection(String schemaName, String tableName, Connection conn) {
        return delegate.getTableMetaFromConnection(schemaName, tableName, conn);
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }
}
