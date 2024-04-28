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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.rplchecker.LogicalTableHashCalculator;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlReplicaHashcheck;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author yudong
 * @since 2023/8/22 15:30
 **/
public class LogicalReplicaHashcheckHandler extends HandlerCommon {
    public LogicalReplicaHashcheckHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext ec) {
        SqlReplicaHashcheck sqlNode = (SqlReplicaHashcheck) ((LogicalDal) logicalPlan).getNativeSqlNode();

        SqlIdentifier tableSource = (SqlIdentifier) sqlNode.getFrom();
        String schema = tableSource.names.get(0);
        String table = tableSource.getLastName();
        final SchemaManager sm = ec.getSchemaManager(schema);
        final TableMeta baseTableMeta = sm.getTable(table);

        List<String> columnList = new ArrayList<>();
        List<ColumnMeta> allColumns = baseTableMeta.getAllColumns();
        for (ColumnMeta column : allColumns) {
            // 隐式主键无法保证上下游一致，所以不校验隐式主键
            if (column.getName().equals("_drds_implicit_id_")) {
                continue;
            }
            columnList.add(column.getName());
        }

        LogicalTableHashCalculator calculator =
            new LogicalTableHashCalculator(schema, table, columnList, sqlNode.getLowerBounds(),
                sqlNode.getUpperBounds(), ec);
        Long hash = calculator.calculateHash();

        ArrayResultCursor result = new ArrayResultCursor("replica hashcheck");
        result.addColumn("hash", DataTypes.LongType);
        result.addRow(new Object[] {hash});
        return result;
    }
}
