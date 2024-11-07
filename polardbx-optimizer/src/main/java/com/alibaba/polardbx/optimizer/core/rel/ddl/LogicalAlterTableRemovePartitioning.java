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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import org.apache.calcite.rel.ddl.AlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTableRemovePartitioning;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableRemovePartitioning extends LogicalAlterTableRepartition {

    private final SqlAlterTableRemovePartitioning sqlAlterTableRemovePartitioning;
    private List<CreateGlobalIndexPreparedData> createGlobalIndexesPreparedData;

    public LogicalAlterTableRemovePartitioning(AlterTableRemovePartitioning alterTableRemovePartitioning) {
        super(alterTableRemovePartitioning);
        this.sqlAlterTableRemovePartitioning = (SqlAlterTableRemovePartitioning) relDdl.sqlNode;
    }

    public static LogicalAlterTableRemovePartitioning create(
            AlterTableRemovePartitioning alterTableRemovePartitioning) {
        return new LogicalAlterTableRemovePartitioning(alterTableRemovePartitioning);
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
                "unarchive table " + schemaName + "." + tableName);
    }

    public void prepareData() {
        createGlobalIndexesPreparedData = new ArrayList<>();

        final List<SqlAddIndex> sqlAddIndexes =
                sqlAlterTableRemovePartitioning.getAlters().stream().map(e -> (SqlAddIndex) e).collect(Collectors.toList());

        for (SqlAddIndex sqlAddIndex : sqlAddIndexes) {
            final String indexName = sqlAddIndex.getIndexName().getLastName();
            CreateGlobalIndexPreparedData preparedGsiData = prepareCreateGsiData(indexName, sqlAddIndex);
            preparedGsiData.setWithImplicitTableGroup(preparedGsiData.getTableGroupName() != null);
            createGlobalIndexesPreparedData.add(preparedGsiData);
        }
    }

    public List<CreateGlobalIndexPreparedData> getCreateGlobalIndexesPreparedData() {
        return createGlobalIndexesPreparedData;
    }
}
