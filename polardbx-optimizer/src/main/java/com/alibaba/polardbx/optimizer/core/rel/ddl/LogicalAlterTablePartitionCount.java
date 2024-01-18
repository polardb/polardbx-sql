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
import org.apache.calcite.rel.ddl.AlterTablePartitionCount;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTablePartitionCount;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTablePartitionCount extends LogicalAlterTableRepartition {

    private final SqlAlterTablePartitionCount sqlAlterTablePartitionCount;
    private List<CreateGlobalIndexPreparedData> createGlobalIndexesPreparedData;

    public LogicalAlterTablePartitionCount(AlterTablePartitionCount alterTableNewPartitionCount) {
        super(alterTableNewPartitionCount);
        this.sqlAlterTablePartitionCount = (SqlAlterTablePartitionCount) relDdl.sqlNode;
    }

    public static LogicalAlterTablePartitionCount create(AlterTablePartitionCount alterTableRepartition) {
        return new LogicalAlterTablePartitionCount(alterTableRepartition);
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    @Override
    public void prepareData() {
        createGlobalIndexesPreparedData = new ArrayList<>();

        final List<SqlAddIndex> sqlAddIndexes =
            sqlAlterTablePartitionCount.getAlters().stream().map(e -> (SqlAddIndex) e).collect(Collectors.toList());

        for (SqlAddIndex sqlAddIndex : sqlAddIndexes) {
            final String indexName = sqlAddIndex.getIndexName().getLastName();
            createGlobalIndexesPreparedData.add(prepareCreateGsiData(indexName, sqlAddIndex));
        }
    }

    public List<CreateGlobalIndexPreparedData> getCreateGlobalIndexesPreparedData() {
        return createGlobalIndexesPreparedData;
    }
}
