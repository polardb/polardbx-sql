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

package com.alibaba.polardbx.repo.mysql.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyQueryCursor;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.metadata.InfoSchemaCommon;

public class LogicalInfoSchemaContext {

    private String targetSchema;
    private boolean systemSchema;
    private boolean schemaRewritten;
    private boolean customSchemaFilter;
    private MyRepository realRepo;
    private OptimizerContext optimizerContext;
    private ExecutorContext executorContext;
    private List<String> logicalTableNames = new ArrayList<>();
    private String countAggFuncTitle;
    private List<ColumnMeta> columnMetas;
    private MyPhyQueryCursor cursor;
    private int numOfTotalCols;
    private int numOfResultCols;
    private int numOfAddedCols;
    private Map<String, String> tableNameMapping = new HashMap<>();
    private Map<String, String> origToAliasMapping = new HashMap<>();
    private Map<String, String> aliasToOrigMapping = new HashMap<>();
    private Map<String, Set<Integer>> paramIndexes;
    private ArrayResultCursor resultCursor;
    private ExecutionContext executionContext;
    private int firstTableSchemaIndex;
    private int firstTableNameIndex;
    private int offset = InfoSchemaCommon.LIMIT_NO_OFFSET_SPECIFIED;
    private int rowCount = InfoSchemaCommon.LIMIT_NO_ROW_COUNT_SPECIFIED;
    private List<Object[]> resultSet = new ArrayList<>();
    private boolean withView = false;

    public LogicalInfoSchemaContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.targetSchema = executionContext.getSchemaName();
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        if (TStringUtil.isNotEmpty(targetSchema)) {
            this.targetSchema = targetSchema;
        }
    }

    public boolean isSystemSchema() {
        return systemSchema;
    }

    public void setSystemSchema(boolean systemSchema) {
        this.systemSchema = systemSchema;
    }

    public boolean isSchemaRewritten() {
        return schemaRewritten;
    }

    public void setSchemaRewritten(boolean schemaRewritten) {
        this.schemaRewritten = schemaRewritten;
    }

    public boolean isCustomSchemaFilter() {
        return customSchemaFilter;
    }

    public void setCustomSchemaFilter(boolean customSchemaFilter) {
        this.customSchemaFilter = customSchemaFilter;
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public ExecutorContext getExecutorContext() {
        return executorContext;
    }

    public MyRepository getRealRepo() {
        return realRepo;
    }

    public void prepareContextAndRepository(IRepository repo) {
        // Prepare optimizer and executor contexts
        optimizerContext = OptimizerContext.getContext(targetSchema);
        executorContext = ExecutorContext.getContext(targetSchema);
        if (optimizerContext == null || executorContext == null) {
            GeneralUtil.nestedException("Cannot find schema: " + targetSchema + ", please check your sql again.");
        }
        // Prepare real repository for cross schema support
        if (TStringUtil.isNotEmpty(targetSchema)
            && !TStringUtil.equalsIgnoreCase(targetSchema, executionContext.getSchemaName())) {
            realRepo = (MyRepository) executorContext.getRepositoryHolder().get(GroupType.MYSQL_JDBC.name());
        } else {
            realRepo = (MyRepository) repo;
        }
    }

    public List<String> getLogicalTableNames() {
        return logicalTableNames;
    }

    public String getCountAggFuncTitle() {
        return countAggFuncTitle;
    }

    public void setCountAggFuncTitle(String countAggFuncTitle) {
        this.countAggFuncTitle = countAggFuncTitle;
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public MyPhyQueryCursor getCursor() {
        return cursor;
    }

    public void setCursor(MyPhyQueryCursor cursor) {
        this.cursor = cursor;
    }

    public int getNumOfTotalCols() {
        return numOfTotalCols;
    }

    public void setNumOfTotalCols(int numOfTotalCols) {
        this.numOfTotalCols = numOfTotalCols;
    }

    public int getNumOfResultCols() {
        return numOfResultCols;
    }

    public void setNumOfResultCols(int numOfResultCols) {
        this.numOfResultCols = numOfResultCols;
    }

    public int getNumOfAddedCols() {
        return numOfAddedCols;
    }

    public void setNumOfAddedCols(int numOfAddedCols) {
        this.numOfAddedCols = numOfAddedCols;
    }

    public Map<String, String> getTableNameMapping() {
        return tableNameMapping;
    }

    public Map<String, String> getOrigToAliasMapping() {
        return origToAliasMapping;
    }

    public void setOrigToAliasMapping(Map<String, String> origToAliasMapping) {
        this.origToAliasMapping = origToAliasMapping;
    }

    public Map<String, String> getAliasToOrigMapping() {
        return aliasToOrigMapping;
    }

    public void setAliasToOrigMapping(Map<String, String> aliasToOrigMapping) {
        this.aliasToOrigMapping = aliasToOrigMapping;
    }

    public Map<String, Set<Integer>> getParamIndexes() {
        return paramIndexes;
    }

    public void setParamIndexes(Map<String, Set<Integer>> paramIndexes) {
        this.paramIndexes = paramIndexes;
    }

    public ArrayResultCursor getResultCursor() {
        return resultCursor;
    }

    public void setResultCursor(ArrayResultCursor resultCursor) {
        this.resultCursor = resultCursor;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    public int getFirstTableSchemaIndex() {
        return firstTableSchemaIndex;
    }

    public void setFirstTableSchemaIndex(int firstTableSchemaIndex) {
        this.firstTableSchemaIndex = firstTableSchemaIndex;
    }

    public int getFirstTableNameIndex() {
        return firstTableNameIndex;
    }

    public void setFirstTableNameIndex(int firstTableNameIndex) {
        this.firstTableNameIndex = firstTableNameIndex;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public List<Object[]> getResultSet() {
        return resultSet;
    }

    public boolean isWithView() {
        return withView;
    }

    public void setWithView(boolean withView) {
        this.withView = withView;
    }
}
