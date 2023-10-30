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

package com.alibaba.polardbx.optimizer.config.schema;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.biv.MockDataManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.optimizer.view.DrdsViewTable;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;

import java.util.List;
import java.util.Map;

/**
 * @author lingce.ldm 2017-12-05 12:59
 */
public class TddlCalciteSchema extends CalciteSchema {

    private final String schemaName;
    private final Map<String, SchemaManager> schemaManagers;

    public TddlCalciteSchema(String schemaName, Map<String, SchemaManager> schemaManagers, CalciteSchema parent,
                             Schema schema,
                             String name) {
        this(schemaName, schemaManagers, parent, schema, name, null, null, null, null, null, null, null);
    }

    private TddlCalciteSchema(String schemaName, Map<String, SchemaManager> schemaManagers, CalciteSchema parent,
                              Schema schema,
                              String name,
                              NameMap<CalciteSchema> subSchemaMap,
                              NameMap<TableEntry> tableMap, NameMap<LatticeEntry> latticeMap,
                              NameMultimap<FunctionEntry> functionMap, NameSet functionNames,
                              NameMap<FunctionEntry> nullaryFunctionMap, List<? extends List<String>> path) {
        super(parent,
            schema,
            name,
            subSchemaMap,
            tableMap,
            latticeMap,
            functionMap,
            functionNames,
            nullaryFunctionMap,
            path);
        this.schemaName = schemaName;
        this.schemaManagers = schemaManagers;
    }

    @Override
    public void setCache(boolean cache) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        final CalciteSchema calciteSchema = new TddlCalciteSchema(name, schemaManagers, this, schema, name);
        subSchemaMap.put(name, calciteSchema);
        return calciteSchema;
    }

    /**
     * 重写该方法,不需要将 TableEntry 保存在 TableMap 中
     */
    @Override
    public TableEntry add(String tableName, Table table, ImmutableList<String> sqls) {
        final TableEntryImpl entry = new TableEntryImpl(this, tableName, table, sqls);
        return entry;
    }

    @Override
    protected CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
        // Check implicit schemas.
        Schema s = schema.getSubSchema(schemaName);
        if (s != null) {
            return new TddlCalciteSchema(schemaName, schemaManagers, this, s, schemaName);
        } else {
            return initDsAndBuildCalciteSchema(schemaName, schemaManagers);
        }
    }

    protected CalciteSchema initDsAndBuildCalciteSchema(String schemaName, Map<String, SchemaManager> schemaManagers) {

        /**
         * get serverConfigManager by optHelper
         */
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager != null) {

            // notify server to init new db(TDataSource) by schema
            Object ds = serverConfigManager.getAndInitDataSourceByDbName(schemaName);
            if (ds == null) {
                return null;
            } else {
                TddlSchema tddlSchema;
                if (schemaManagers != null && schemaManagers.containsKey(schemaName)) {
                    tddlSchema = new TddlSchema(schemaName, schemaManagers.get(schemaName));
                } else {
                    if (OptimizerContext.getContext(schemaName) == null) {
                        return null;
                    }
                    tddlSchema =
                        new TddlSchema(schemaName, OptimizerContext.getContext(schemaName).getLatestSchemaManager());
                }
                return new TddlCalciteSchema(schemaName, schemaManagers, this, tddlSchema, schemaName);
            }
        }
        return null;
    }

    public static class ViewProtoDataType implements RelProtoDataType {

        private String schemaName;
        private final List<String> columnList;

        private final String viewDefinition;

        public ViewProtoDataType(String schemaName, List<String> columnList, String viewDefinition) {
            this.schemaName = schemaName;
            this.columnList = columnList;
            this.viewDefinition = viewDefinition;
        }

        @Override
        public RelDataType apply(RelDataTypeFactory factory) {
            SqlNode ast = new FastsqlParser().parse(viewDefinition).get(0);
            SqlConverter converter = SqlConverter.getInstance(new ExecutionContext());
            SqlNode validatedNode = converter.validate(ast);
            RelDataType rowType = converter.toRel(validatedNode).getRowType();

            if (columnList == null) {
                return rowType;
            }

            final ImmutableList.Builder<RelDataTypeField> list =
                ImmutableList.builder();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String column = columnList.get(i);
                RelDataTypeField field = rowType.getFieldList().get(i);
                list.add(new RelDataTypeFieldImpl(column, i, field.getType()));
            }

            return new RelRecordType(StructKind.FULLY_QUALIFIED, list.build());
        }
    }

    public static class VirtualViewProtoDataType implements RelProtoDataType {

        private VirtualViewType virtualViewType;

        public VirtualViewProtoDataType(String schemaName, VirtualViewType virtualViewType) {
            this.virtualViewType = virtualViewType;
        }

        @Override
        public RelDataType apply(RelDataTypeFactory factory) {
            ExecutionContext ec = new ExecutionContext();
            return VirtualView
                .create(SqlConverter.getInstance(ec).createRelOptCluster(new PlannerContext(ec)), virtualViewType)
                .getRowType();
        }
    }

    @Override
    protected TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        // Check implicit tables.
        Table table;

        try {
            // use logical table name to acquaire table meta in cache
//            if (ConfigDataMode.isFastMock()) {
//                tableName = MockDataManager.phyTableToLogicalTableName.get(tableName.toLowerCase());
//            }
            table = schema.getTable(tableName);
            if (table instanceof TableMeta && ((TableMeta) table).getStatus() != TableStatus.PUBLIC) {
                throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
            }
        } catch (Throwable t) {
            if (OptimizerContext.getContext(schemaName) == null) {
                throw new RuntimeException(t);
            }
            SystemTableView.Row row = OptimizerContext.getContext(schemaName).getViewManager().select(tableName);
            if (row != null) {
                String viewDefinition = row.getViewDefinition();
                List<String> columnList = row.getColumnList();
                RelProtoDataType relProtoDataType;
                if (row.isVirtual()) {
                    VirtualViewType virtualViewType = row.getVirtualViewType();
                    relProtoDataType = new VirtualViewProtoDataType(schemaName, virtualViewType);
                } else {
                    relProtoDataType = new ViewProtoDataType(schemaName, columnList, viewDefinition);
                }
                table = new DrdsViewTable(null, relProtoDataType, row, ImmutableList.<String>of(), null);
            } else if (t instanceof TddlRuntimeException) {
                throw t;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_CANNOT_FETCH_TABLE_META, t, tableName, t.getMessage());
            }
        }

        if (table != null) {
            return tableEntry(tableName, table);
        }
        return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
        ImmutableSortedMap<String, CalciteSchema> explicitSubSchemas = builder.build();
        for (String schemaName : schema.getSubSchemaNames()) {
            if (explicitSubSchemas.containsKey(schemaName)) {
                // explicit subschema wins.
                continue;
            }
            Schema s = schema.getSubSchema(schemaName);
            if (s != null) {
                CalciteSchema calciteSchema = new TddlCalciteSchema(schemaName, schemaManagers, this, s, schemaName);
                builder.put(schemaName, calciteSchema);
            }
        }
    }

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getTableNames());
    }

    @Override
    protected void addImplicitFunctionsToBuilder(Builder<Function> builder, String name, boolean caseSensitive) {

    }

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getFunctionNames());
    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
        ImmutableSortedMap.Builder<String, Table> builder) {
        ImmutableSortedMap<String, Table> explicitTables = builder.build();

        for (String s : schema.getFunctionNames()) {
            // explicit table wins.
            if (explicitTables.containsKey(s)) {
                continue;
            }
            for (Function function : schema.getFunctions(s)) {
                if (function instanceof TableMacro && function.getParameters().isEmpty()) {
                    final Table table = ((TableMacro) function).apply(ImmutableList.of());
                    builder.put(s, table);
                }
            }
        }
    }

    @Override
    protected CalciteSchema snapshot(CalciteSchema parent, SchemaVersion version) {
        return null;
    }

    @Override
    protected TableEntry getImplicitTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
        for (String s : schema.getFunctionNames()) {
            for (Function function : schema.getFunctions(s)) {
                if (function instanceof TableMacro && function.getParameters().isEmpty()) {
                    final Table table = ((TableMacro) function).apply(ImmutableList.of());
                    return tableEntry(tableName, table);
                }
            }
        }
        return null;
    }

    @Override
    protected boolean isCacheEnabled() {
        return false;
    }
}
