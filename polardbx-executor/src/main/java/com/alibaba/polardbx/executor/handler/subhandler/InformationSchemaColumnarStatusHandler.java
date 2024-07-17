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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaColumnarStatus;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class InformationSchemaColumnarStatusHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaColumnarStatusHandler.class);

    public InformationSchemaColumnarStatusHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaColumnarStatus;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        final int tsoIndex = InformationSchemaColumnarStatus.getTsoIndex();
        final int schemaIndex = InformationSchemaColumnarStatus.getSchemaIndex();
        final int tableIndex = InformationSchemaColumnarStatus.getTableIndex();
        final int indexIndex = InformationSchemaColumnarStatus.getIndexNameIndex();

        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        // Tso
        Long tso = null;
        Set<String> tsoFilter = virtualView.getEqualsFilterValues(tsoIndex, params);
        if (tsoFilter.isEmpty()) {
            //默认最新tso
            tso = ColumnarTransactionUtils.getLatestShowColumnarStatusTsoFromGms();
        } else {
            tso = Long.valueOf(tsoFilter.iterator().next());
        }

        if (tso == null) {
            tso = Long.MIN_VALUE;
        }

        //1、获取所有columnar index表
        List<ColumnarTableMappingRecord> columnarRecords = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {

            ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
            tableMappingAccessor.setConnection(metaDbConn);

            //同时显示正在创建的表，方便查看进度
            columnarRecords.addAll(tableMappingAccessor.queryByStatus(ColumnarTableStatus.PUBLIC.name()));
            columnarRecords.addAll(tableMappingAccessor.queryByStatus(ColumnarTableStatus.CREATING.name()));

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e, "Fail to fetch columnar index");
        }
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        Set<String> tableNames = new TreeSet<>(String::compareToIgnoreCase);
        Set<String> indexNames = new TreeSet<>(String::compareToIgnoreCase);

        for (ColumnarTableMappingRecord record : columnarRecords) {
            schemaNames.add(record.tableSchema);
            tableNames.add(record.tableName);
            indexNames.add(record.indexName);
        }

        //过滤条件，确定需要统计的index
        schemaNames = virtualView.applyFilters(schemaIndex, params, schemaNames);
        tableNames = virtualView.applyFilters(tableIndex, params, tableNames);
        indexNames = virtualView.applyFilters(indexIndex, params, indexNames);

        List<ColumnarTableMappingRecord> needReadIndexRecords = new ArrayList<>();
        for (ColumnarTableMappingRecord record : columnarRecords) {
            if (schemaNames.contains(record.tableSchema)
                && tableNames.contains(record.tableName)
                && indexNames.contains(record.indexName)) {
                needReadIndexRecords.add(record);
            }
        }

        List<ColumnarTransactionUtils.ColumnarIndexStatusRow> rows =
            ColumnarTransactionUtils.queryColumnarIndexStatus(tso, needReadIndexRecords);

        rows.forEach(row -> cursor.addRow(new Object[] {
            row.tso,
            row.tableSchema,
            row.tableName,
            row.indexName,
            row.indexId,
            row.partitionNum,
            row.csvFileNum,
            row.csvRows,
            row.csvFileSize,
            row.orcFileNum,
            row.orcRows,
            row.orcFileSize,
            row.delFileNum,
            row.delRows,
            row.delFileSize,
            row.csvRows + row.orcRows - row.delRows,
            row.csvFileSize + row.orcFileSize + row.delFileSize,
            row.status
        }));

        return cursor;
    }
}
