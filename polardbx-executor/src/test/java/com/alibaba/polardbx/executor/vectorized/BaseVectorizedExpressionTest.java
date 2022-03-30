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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilders;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.MockExec;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.SimpleSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.MockStatisticDatasource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ToDrdsRelVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.VirtualTableRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BaseVectorizedExpressionTest {
    protected static final String APP_NAME = "obtest";
    private static final String TABLE_DDL = "CREATE TABLE `t_table` (\n"
        + "  `id` int(11) NOT NULL,\n"
        + "  `test_double` DOUBLE DEFAULT NULL,\n"
        + "  `test_double2` DOUBLE DEFAULT NULL,\n"
        + "  `test_float` FLOAT DEFAULT NULL,\n"
        + "  `test_float2` FLOAT DEFAULT NULL,\n"
        + "  `test_tinyint` TINYINT DEFAULT NULL,\n"
        + "  `test_tinyint2` TINYINT DEFAULT NULL,\n"
        + "  `test_utinyint` TINYINT UNSIGNED DEFAULT NULL,\n"
        + "  `test_utinyint2` TINYINT UNSIGNED DEFAULT NULL,\n"
        + "  `test_smallint` SMALLINT DEFAULT NULL,\n"
        + "  `test_smallint2` SMALLINT DEFAULT NULL,\n"
        + "  `test_usmallint` SMALLINT UNSIGNED DEFAULT NULL,\n"
        + "  `test_usmallint2` SMALLINT UNSIGNED DEFAULT NULL,\n"
        + "  `test_mediumint` MEDIUMINT DEFAULT NULL,\n"
        + "  `test_mediumint2` MEDIUMINT DEFAULT NULL,\n"
        + "  `test_umediumint` MEDIUMINT UNSIGNED DEFAULT NULL,\n"
        + "  `test_umediumint2` MEDIUMINT UNSIGNED DEFAULT NULL,\n"
        + "  `test_integer` INT DEFAULT NULL,\n"
        + "  `test_integer2` INT DEFAULT NULL,\n"
        + "  `test_uinteger` INT UNSIGNED DEFAULT NULL,\n"
        + "  `test_uinteger2` INT UNSIGNED DEFAULT NULL,\n"
        + "  `test_bigint` BIGINT DEFAULT NULL,\n"
        + "  `test_bigint2` BIGINT DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`)\n"
        + "  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    private FastsqlParser parser;
    private OptimizerContext optimizerContext;
    private TableMeta tableMeta;
    private Map<ColumnMeta, Block> tableData;
    private ExecutionContext executionContext;

    public static Block output(DataType<?> dataType, Object... data) {
        BlockBuilder builder = BlockBuilders.create(dataType, new ExecutionContext());
        Arrays.stream(data)
            .map(dataType::convertFrom)
            .forEach(builder::writeObject);

        return builder.build();
    }

    public static Block copySelected(Block input, int[] selection) {
        BlockBuilder builder = BlockBuilders.create(((RandomAccessBlock) input).getType(), new ExecutionContext());
        Arrays.stream(selection)
            .mapToObj(input::getObject)
            .forEach(builder::writeObject);
        return builder.build();
    }

    public String getAppName() {
        return APP_NAME;
    }

    @Before
    public void setup() {
        parser = new FastsqlParser();
        initOptimizerContext();
        buildTable();
        initTableData();
        executionContext = new ExecutionContext();
    }

    private void initOptimizerContext() {
        optimizerContext = new OptimizerContext(APP_NAME);

        PartitionInfoManager partInfoMgr = new PartitionInfoManager(APP_NAME, APP_NAME, true);
        TableGroupInfoManager tableGroupInfoManager = new TableGroupInfoManager(APP_NAME);

        TddlRule tddlRule = new TddlRule();
        tddlRule.setAppName(APP_NAME);
        tddlRule.setAllowEmptyRule(true);
        tddlRule.setDefaultDbIndex("optest_0000");
        TddlRuleManager rule = new TddlRuleManager(tddlRule, partInfoMgr, tableGroupInfoManager, APP_NAME);

        List<Group> groups = new LinkedList<>();
        Matrix matrix = new Matrix();
        matrix.setGroups(groups);

        SimpleSchemaManager sm = new SimpleSchemaManager(APP_NAME, rule);

        optimizerContext.setMatrix(matrix);
        optimizerContext.setRuleManager(rule);
        optimizerContext.setSchemaManager(sm);

        OptimizerContext.loadContext(optimizerContext);

        StatisticManager statisticManager = new StatisticManager(APP_NAME, new MockStatisticDatasource());

        optimizerContext.setStatisticManager(statisticManager);

        OptimizerContext.loadContext(optimizerContext);
    }

    private void buildTable() {
        final MySqlCreateTableStatement stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(TABLE_DDL).get(0);
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        tableMeta = new TableMetaParser().parse(stat, new ExecutionContext());

        final SqlCreateTable sqlCreateTable =
            (SqlCreateTable) FastsqlParser.convertStatementToSqlNode(stat, null, executionContext);
        RelUtils.stringValue(sqlCreateTable.getName());

        // init table rule
        final TableRule tr = new TableRule();
        tr.setDbNamePattern(
            OptimizerContext.getContext(APP_NAME).getRuleManager().getTddlRule().getDefaultDbIndex());
        tr.setTbNamePattern(tableMeta.getTableName());
        tr.init();

        VirtualTableRoot.setTestRule(tableMeta.getTableName(), tr);
        SchemaManager sm = optimizerContext.getLatestSchemaManager();
        sm.putTable(tableMeta.getTableName(), tableMeta);
    }

    private void initTableData() {
        List<ColumnMeta> columnMetas = tableMeta.getAllColumns();
        tableData = new HashMap<>(columnMetas.size());

        // Init first column, here we assume that first column is primary key and int type
        {
            BlockBuilder blockBuilder = BlockBuilders.create(columnMetas.get(0).getDataType(), new ExecutionContext());
            for (int i = 0; i < VectorizedExpressionTestUtils.TEST_ROW_COUNT; i++) {
                blockBuilder.writeInt(i);
            }
            tableData.put(columnMetas.get(0), blockBuilder.build());
        }

        // Build other table data with 0
        for (int i = 1; i < columnMetas.size(); i++) {
            ColumnMeta columnMeta = columnMetas.get(i);
            BlockBuilder blockBuilder = BlockBuilders.create(columnMetas.get(i).getDataType(), new ExecutionContext());
            for (int j = 0; j < VectorizedExpressionTestUtils.TEST_ROW_COUNT; j++) {
                blockBuilder.appendNull();
            }
            tableData.put(columnMeta, blockBuilder.build());
        }
    }

    protected RelNode sql2Plan(String sql) {
        SqlNodeList astList = parser.parse(sql);

        SqlNode ast = astList.get(0);

        executionContext.setParams(new Parameters());
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_DIRECT_PLAN, false);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);

        // validate
        SqlConverter converter = SqlConverter.getInstance(plannerContext.getSchemaName(), executionContext);
        SqlNode validatedNode = converter.validate(ast);

        // sqlNode to relNode
        RelNode relNode = converter.toRel(validatedNode, plannerContext);

        // relNode to drdsRelNode
        ToDrdsRelVisitor toDrdsRelVisitor = new ToDrdsRelVisitor(validatedNode, plannerContext);
        return relNode.accept(toDrdsRelVisitor);
    }

    protected void updateColumn(ColumnInput input) {
        ColumnMeta columnMeta = tableMeta.getColumn(input.getColumnName());
        if (columnMeta == null) {
            throw new IllegalArgumentException("Column " + input + " not found!");
        }

        BlockBuilder columnInput = BlockBuilders.create(columnMeta.getDataType(), new ExecutionContext());
        Arrays.stream(input.getData())
            .map(d -> columnMeta.getDataType().convertFrom(d))
            .forEach(columnInput::writeObject);
        tableData.put(columnMeta, columnInput.build());
    }

    protected void updateColumns(List<ColumnInput> inputs) {
        inputs.forEach(this::updateColumn);
    }

    protected MockExec buildInputFromTableData(int[] selection) {
        List<DataType> types = tableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getDataType)
            .map(t -> (DataType<?>) t)
            .collect(Collectors.toList());

        Block[] blocks = tableMeta.getAllColumns()
            .stream()
            .map(tableData::get)
            .toArray(Block[]::new);

        Chunk chunk;
        if (selection != null) {
            chunk = new Chunk(selection.length, blocks);
            chunk.setSelection(selection);
        } else {
            chunk = new Chunk(blocks);
        }

        return new MockExec(types, Collections.singletonList(chunk));
    }

    protected ExecutionContext getExecutionContext() {
        return executionContext;
    }
}