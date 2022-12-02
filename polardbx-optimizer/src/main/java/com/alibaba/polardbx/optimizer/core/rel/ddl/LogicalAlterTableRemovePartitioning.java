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

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import org.apache.calcite.rel.ddl.AlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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

    public void prepareData() {
        createGlobalIndexesPreparedData = new ArrayList<>();

        final List<SqlAddIndex> sqlAddIndexes =
            sqlAlterTableRemovePartitioning.getAlters().stream().map(e -> (SqlAddIndex) e).collect(Collectors.toList());

        for (SqlAddIndex sqlAddIndex : sqlAddIndexes) {
            final String indexName = sqlAddIndex.getIndexName().getLastName();
            createGlobalIndexesPreparedData.add(prepareCreateGsiData(indexName, sqlAddIndex));
        }
    }

    public List<CreateGlobalIndexPreparedData> getCreateGlobalIndexesPreparedData() {
        return createGlobalIndexesPreparedData;
    }
}
