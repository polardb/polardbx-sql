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

package com.alibaba.polardbx.optimizer.view;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

public class InformationSchemaStorageStatus extends VirtualView {

    private static final int FIELD_COUNT = 10;

    public static final String[] STORAGE_STATUS_ITEM = {
        "STORAGE_INST_ID", "INST_ROLE", "INST_KIND", "COM_SELECT", "COM_INSERT",
        "COM_UPDATE", "COM_DELETE", "COM_BEGIN", "COM_COMMIT", "COM_SET_OPTION", "COM_RESET", "COM_XA_COMMIT",
        "COM_XA_END", "COM_XA_PREPARE", "COM_XA_RECOVER", "COM_XA_ROLLBACK", "COM_XA_START",
        "INNODB_BUFFER_POOL_READ_REQUESTS", "INNODB_BUFFER_POOL_READS", "INNODB_ROWS_INSERTED",
        "INNODB_ROWS_UPDATED", "INNODB_ROWS_DELETED", "INNODB_ROWS_READ", "THREADS_RUNNING",
        "THREADS_CONNECTED", "THREADS_CACHED", "THREADS_CREATED", "BYTES_RECEIVED", "BYTES_SENT",
        "INNODB_BUFFER_POOL_PAGES_DATA", "INNODB_BUFFER_POOL_PAGES_FREE", "INNODB_BUFFER_POOL_PAGES_DIRTY",
        "INNODB_BUFFER_POOL_PAGES_FLUSHED", "INNODB_DATA_READS", "INNODB_DATA_WRITES", "INNODB_DATA_READ",
        "INNODB_DATA_WRITTEN", "INNODB_DATA_FSYNCS", "INNODB_OS_LOG_FSYNCS", "INNODB_OS_LOG_WRITTEN"};

    protected InformationSchemaStorageStatus(RelOptCluster cluster,
                                             RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.STORAGE_STATUS);
    }

    protected InformationSchemaStorageStatus(RelOptCluster cluster,
                                             RelTraitSet traitSet, VirtualViewType viewType) {
        super(cluster, traitSet, viewType);
    }

    public InformationSchemaStorageStatus(RelInput input) {
        super(input);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        for (int i = 0; i < STORAGE_STATUS_ITEM.length; i++) {
            columns.add(
                new RelDataTypeFieldImpl(STORAGE_STATUS_ITEM[i], 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        }
        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        return i == getTableStorageInstIdIndex() || i == getTableInstRoleIndex() || i == getTableInstKindIndex();
    }

    private static final List<Integer> INDEXABLE_COLUMNS;

    static {
        INDEXABLE_COLUMNS =
            Lists.newArrayList(getTableStorageInstIdIndex(), getTableInstRoleIndex(), getTableInstRoleIndex());
    }

    @Override
    List<Integer> indexableColumnList() {
        return INDEXABLE_COLUMNS;
    }

    static public int getTableStorageInstIdIndex() {
        return 0;
    }

    static public int getTableInstRoleIndex() {
        return 1;
    }

    static public int getTableInstKindIndex() {
        return 2;
    }
}
