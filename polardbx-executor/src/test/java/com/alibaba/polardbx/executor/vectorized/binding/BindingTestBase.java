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

package com.alibaba.polardbx.executor.vectorized.binding;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.MockStatisticDatasource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.schema.RootSchemaFactory;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlValidator;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.view.DrdsViewExpander;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class BindingTestBase {
    protected static final String SCHEMA_NAME = "type_test";
    protected static final List<String> FIELDS = ImmutableList.of(
        "integer_test",
        "u_integer_test",
        "tinyint_test",
        "u_tinyint_test",
        "smallint_test",
        "u_smallint_test",
        "mediumint_test",
        "u_mediumint_test",
        "bigint_test",
        "u_bigint_test",
        "varchar_test",
        "char_test",
        "blob_test",
        "tinyint_1bit_test",
        "bit_test",
        "float_test",
        "double_test",
        "decimal_test",
        "date_test",
        "time_test",
        "datetime_test",
        "timestamp_test",
        "year_test",
        "mediumtext_test"
    );
    protected static final List<String> LITERALS = ImmutableList.of(
        "1",
        "\'a\'"
    );
    protected static final List<String> VECTORIZED_FIELDS = ImmutableList.of(
        // int types
        "integer_test",
        "u_integer_test",
        "tinyint_test",
        "u_tinyint_test",
        "smallint_test",
        "u_smallint_test",
        "mediumint_test",
        "u_mediumint_test",
        "bigint_test",

        // approximate types
        "float_test",
        "double_test"
    );
    protected static final String TABLE_DDL = "CREATE TABLE `test_is` (\n"
        + "  `pk` bigint(11) NOT NULL auto_increment,\n"
        + "  `integer_test` int(11) DEFAULT NULL,\n"
        + "  `u_integer_test` int(11) UNSIGNED DEFAULT NULL,\n"
        + "  `varchar_test` varchar(255) DEFAULT NULL,\n"
        + "  `char_test` char(255) DEFAULT NULL,\n"
        + "  `blob_test` blob,\n"
        + "  `tinyint_test` tinyint(4) DEFAULT NULL,\n"
        + "  `u_tinyint_test` tinyint(4) UNSIGNED DEFAULT NULL,\n"
        + "  `tinyint_1bit_test` tinyint(1) DEFAULT NULL,\n"
        + "  `smallint_test` smallint(6) DEFAULT NULL,\n"
        + "  `u_smallint_test` smallint(6) UNSIGNED DEFAULT NULL,\n"
        + "  `mediumint_test` mediumint(9) DEFAULT NULL,\n"
        + "  `u_mediumint_test` mediumint(9) UNSIGNED DEFAULT NULL,\n"
        + "  `bit_test` bit(1) DEFAULT NULL,\n"
        + "  `bigint_test` bigint(20) DEFAULT NULL,\n"
        + "  `u_bigint_test` bigint(20) UNSIGNED DEFAULT NULL,\n"
        + "  `float_test` float DEFAULT NULL,\n"
        + "  `double_test` double DEFAULT NULL,\n"
        + "  `decimal_test` decimal(10,0) DEFAULT NULL,\n"
        + "  `date_test` date DEFAULT NULL,\n"
        + "  `time_test` time DEFAULT NULL,\n"
        + "  `datetime_test` datetime DEFAULT NULL,\n"
        + "  `timestamp_test` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
        + "  `year_test` year(4) DEFAULT NULL,\n"
        + "  `mediumtext_test` mediumtext,\n"
        + "  PRIMARY KEY (`pk`)\n"
        + ");";
    final protected RelDataTypeFactory typeFactory;
    final protected SqlParser.Config parserConfig;
    final protected ExecutionContext executionContext;
    final protected FastsqlParser fp;
    final protected SchemaManager schemaManager;

    protected TddlValidator validator;
    protected SqlToRelConverter sqlToRelConverter;

    protected Map<String, String> expectedPlans;

    protected BindingTestBase() {
        this.executionContext = new ExecutionContext();
        this.fp = new FastsqlParser();
        this.typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        this.parserConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).setParserFactory(SqlParserImpl.FACTORY).build();
        this.schemaManager = new MockSchemaManager();
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        initContext();
        initTable("test", TABLE_DDL);
    }

    protected void loadPlans() {
        String file = this.getClass().getSimpleName() + ".yml";
        InputStream in = this.getClass().getResourceAsStream(file);

        Yaml yaml = new Yaml();
        expectedPlans = yaml.load(in);
    }

    protected void compare(int index, String sql, String tree) {
        String sqlKey = "sql" + index;
        String treeKey = "tree" + index;
        Assert.assertEquals("the sql is not matched, the expect is \n"
                + expectedPlans.get(sqlKey) + ", \nbut the actual is \n" + sql,
            expectedPlans.get(sqlKey).trim(),
            sql.trim());
        Assert.assertEquals("the tree is not matched, the expect is \n"
                + expectedPlans.get(treeKey) + ", \nbut the actual is \n" + tree,
            expectedPlans.get(treeKey).trim(),
            tree.trim());
    }

    protected void initContext() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);

        StatisticManager.sds = MockStatisticDatasource.getInstance();

        // prepare optimizer context
        OptimizerContext context = new OptimizerContext(SCHEMA_NAME);
        OptimizerContext.loadContext(context);
        context.setSchemaManager(schemaManager);

        String schemaName = OptimizerContext.getContext(SCHEMA_NAME).getSchemaName();
        CalciteSchema calciteSchema = RootSchemaFactory.createRootSchema(schemaName, executionContext);
        CalciteCatalogReader catalog = new CalciteCatalogReader(calciteSchema,
            calciteSchema.path(schemaName),
            new TddlJavaTypeFactoryImpl(),
            connectionConfig);

        this.validator =
            new TddlValidator(TddlOperatorTable.instance(), catalog, typeFactory, SqlConformanceEnum.MYSQL_5);

        validator.setEnableTypeCoercion(true);
        validator.setTypeCoercion(new TypeCoercionImpl(typeFactory, validator));
        validator.setDefaultNullCollation(NullCollation.LOW);
        validator.setIdentifierExpansion(false);
        validator.setCallRewrite(false);

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .build();

        PlannerContext plannerContext = PlannerContext.EMPTY_CONTEXT;
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCostFactory costFactory = DrdsRelOptCostImpl.FACTORY;
        RelOptPlanner planner = new VolcanoPlanner(costFactory, plannerContext);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);

        this.sqlToRelConverter = new TddlSqlToRelConverter(
            new DrdsViewExpander(cluster, SqlConverter.getInstance(executionContext), catalog),
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            PlannerContext.getPlannerContext(cluster));
    }

    protected void initTable(String tableName, String ddlSql) {
        final MySqlCreateTableStatement stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(ddlSql).get(0);
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        final TableMeta tm = new TableMetaParser().parse(stat, executionContext);
        schemaManager.putTable(tableName, tm);
    }

    protected Tester testProject(String sql) {
        return new Tester(sql, false, Tester.TEST_PROJECT);
    }

    protected Tester testFilter(String sql) {
        return new Tester(sql, false, Tester.TEST_FILTER);
    }

    protected Tester testProject(String sql, boolean parameterized) {
        return new Tester(sql, parameterized, Tester.TEST_PROJECT);
    }

    protected Tester testFilter(String sql, boolean parameterized) {
        return new Tester(sql, parameterized, Tester.TEST_FILTER);
    }

    private SqlNode validate(String sql, List<Object> params) {
        SqlNodeList astList = params == null ? fp.parse(sql) : fp.parse(sql, params);
        SqlNode ast = astList.get(0);
        SqlNode validatedAst = validator.validate(ast);
        return validatedAst;
    }

    protected class Tester {
        protected static final int TEST_PROJECT = 0;
        protected static final int TEST_FILTER = 1;
        private RelNode relNode;
        private List<VectorizedExpression> expressions;
        private List<String> trees;

        public Tester(String sql, boolean parameterized, int mode) {
            Preconditions.checkArgument(mode == TEST_FILTER || mode == TEST_PROJECT);

            SqlNode validatedAst;
            if (parameterized) {
                Map<Integer, ParameterContext> currentParameter = new HashMap<>();
                SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql, currentParameter, false);
                validatedAst = validate(sqlParameterized.getSql(), sqlParameterized.getParameters());
            } else {
                validatedAst = validate(sql, null);
            }

            RelRoot relRoot = sqlToRelConverter.convertQuery(validatedAst, false, true);
            relNode = relRoot.rel;

            Preconditions.checkArgument(relNode instanceof LogicalProject
                    && relNode.getInputs().size() == 1,
                "the sql is not adaptive to this tester");

            LogicalProject project = (LogicalProject) relNode;
            LogicalFilter filter = null;
            RelNode input = null;

            if (mode == TEST_PROJECT) {
                input = relNode.getInput(0);
            } else if (mode == TEST_FILTER) {
                filter = (LogicalFilter) relNode.getInput(0);
                input = filter.getInput(0);
            }

            // get input types from rel data type
            List<DataType<?>> inputTypes = input.getRowType()
                .getFieldList()
                .stream()
                .map(f -> new Field(f.getType()).getDataType())
                .map(d -> (DataType<?>) d)
                .collect(Collectors.toList());

            if (mode == TEST_PROJECT) {
                this.expressions = project.getProjects()
                    .stream()
                    .map(e -> VectorizedExpressionBuilder.buildVectorizedExpression(inputTypes, e, executionContext)
                        .getKey())
                    .collect(Collectors.toList());
            } else if (mode == TEST_FILTER) {
                this.expressions = ImmutableList.of(
                    VectorizedExpressionBuilder.buildVectorizedExpression(
                            inputTypes,
                            filter.getCondition(),
                            executionContext)
                        .getKey()
                );
            }

            this.trees = expressions
                .stream()
                .map(e -> VectorizedExpressionUtils.digest(e))
                .collect(Collectors.toList());
        }

        public void tree(String tree) {
            boolean match = trees.stream().anyMatch(t -> t.equals(tree));
            Assert.assertTrue(match);
        }

        public String trees() {
            StringBuilder builder = new StringBuilder();
            trees.forEach(t -> builder.append(t).append("\n"));
            if (builder.length() > 0) {
                builder.setLength(builder.length() - 1);
            }
            return builder.toString();
        }

        public void showTrees() {
            System.out.println(trees());
        }
    }

    private class MockSchemaManager implements SchemaManager {
        private Map<String, TableMeta> tableMetaMap = new HashMap<>();

        @Override
        public TableMeta getTable(String tableName) {
            if (tableMetaMap.containsKey(tableName.toUpperCase())) {
                return tableMetaMap.get(tableName.toUpperCase());
            } else {
                String error = String.format("Table %s not exists, existing tables: %s, hashcode: %s",
                    tableName.toUpperCase(),
                    StringUtils.join(tableMetaMap.keySet().toArray(), " ,"),
                    this.hashCode());
                throw new RuntimeException(error);
            }
        }

        @Override
        public void putTable(String tableName, TableMeta tableMeta) {
            tableMetaMap.put(tableName.toUpperCase(), tableMeta);
        }

        @Override
        public void reload(String tableName) {

        }

        @Override
        public void invalidate(String tableName) {

        }

        @Override
        public void invalidateAll() {
            tableMetaMap.clear();
        }

        @Override
        public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
            return null;
        }

        @Override
        public String getSchemaName() {
            return "test";
        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public boolean isInited() {
            return true;
        }
    }
}