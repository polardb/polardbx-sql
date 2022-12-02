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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.schema.RootSchemaFactory;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.MockStatisticDatasource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlValidator;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.view.DrdsViewExpander;
import com.google.common.base.Preconditions;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Before;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class DataTypeTestBase {
    protected static final String SCHEMA_NAME = "type_test";
    protected static final String TABLE_DDL = "CREATE TABLE `test` (\n"
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

    private static final String COLLATION_TEST_DDL = "create table collation_test (\n"
        + "    pk int(11) auto_increment,\n"
        + "    v_default varchar(255) not null default 'abc',\n"
        + "    v_utf8mb4 varchar(255) character set utf8mb4 not null default 'abc',\n"
        + "    v_utf8mb4_general_ci varchar(255) character set utf8mb4 collate utf8mb4_general_ci not null default 'abc',\n"
        + "    v_utf8mb4_unicode_ci varchar(255) character set utf8mb4 collate utf8mb4_unicode_ci not null default 'abc',\n"
        + "    v_utf8mb4_bin varchar(255) character set utf8mb4 collate utf8mb4_bin not null default 'abc',\n"
        + "    v_binary varchar(255) character set binary not null default 'abc',\n"
        + "\n"
        + "    v_ascii_bin varchar(255) character set ascii collate ascii_bin not null default 'abc',\n"
        + "    v_ascii_general_ci varchar(255) character set ascii collate ascii_general_ci not null default 'abc',\n"
        + "    v_ascii varchar(255) character set ascii not null default 'abc',\n"
        + "\n"
        + "    v_utf16 varchar(255) character set utf16 not null default 'abc',\n"
        + "    v_utf16_bin varchar(255) character set utf16 collate utf16_bin not null default 'abc',\n"
        + "    v_utf16_general_ci varchar(255) character set utf16 collate utf16_general_ci not null default 'abc',\n"
        + "    v_utf16_unicode_ci varchar(255) character set utf16 collate utf16_unicode_ci not null default 'abc',\n"
        + "    v_utf32 varchar(255) character set utf32 not null default 'abc',\n"
        + "    v_utf32_bin varchar(255) character set utf32 collate utf32_bin not null default 'abc',\n"
        + "    v_utf32_general_ci varchar(255) character set utf32 collate utf32_general_ci not null default 'abc',\n"
        + "    v_utf32_unicode_ci varchar(255) character set utf32 collate utf32_unicode_ci not null default 'abc',\n"
        + "    v_utf8 varchar(255) character set utf8 not null default 'abc',\n"
        + "    v_utf8_bin varchar(255) character set utf8 collate utf8_bin not null default 'abc',\n"
        + "    v_utf8_general_ci varchar(255) character set utf8 collate utf8_general_ci not null default 'abc',\n"
        + "    v_utf8_unicode_ci varchar(255) character set utf8 collate utf8_unicode_ci not null default 'abc',\n"
        + "    v_utf16le varchar(255) character set utf16le not null default 'abc',\n"
        + "    v_utf16le_bin varchar(255) character set utf16le collate utf16le_bin not null default 'abc',\n"
        + "    v_utf16le_general_ci varchar(255) character set utf16le collate utf16le_general_ci not null default 'abc',\n"
        + "    v_latin1 varchar(255) character set latin1 not null default 'abc',\n"
        + "    v_latin1_swedish_ci varchar(255) character set latin1 collate latin1_swedish_ci not null default 'abc',\n"
        + "    v_latin1_german1_ci varchar(255) character set latin1 collate latin1_german1_ci not null default 'abc',\n"
        + "    v_latin1_danish_ci varchar(255) character set latin1 collate latin1_danish_ci not null default 'abc',\n"
        + "    v_latin1_german2_ci varchar(255) character set latin1 collate latin1_german2_ci not null default 'abc',\n"
        + "    v_latin1_bin varchar(255) character set latin1 collate latin1_bin not null default 'abc',\n"
        + "    v_latin1_general_ci varchar(255) character set latin1 collate latin1_general_ci not null default 'abc',\n"
        + "    v_latin1_general_cs varchar(255) character set latin1 collate latin1_general_cs not null default 'abc',\n"
        + "    v_latin1_spanish_ci varchar(255) character set latin1 collate latin1_spanish_ci not null default 'abc',\n"
        + "    v_gbk varchar(255) character set gbk not null default 'abc',\n"
        + "    v_gbk_chinese_ci varchar(255) character set gbk collate gbk_chinese_ci not null default 'abc',\n"
        + "    v_gbk_bin varchar(255) character set gbk collate gbk_bin not null default 'abc',\n"
        + "    v_big5 varchar(255) character set big5 not null default 'abc',\n"
        + "    v_big5_chinese_ci varchar(255) character set big5 collate big5_chinese_ci not null default 'abc',\n"
        + "    v_big5_bin varchar(255) character set big5 collate big5_bin not null default 'abc',\n"
        + "    primary key (pk)\n"
        + ") default character set latin1 collate latin1_swedish_ci dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 4;";

    private static Field nodeTypes;
    protected SchemaManager schemaManager;
    protected RelDataTypeFactory typeFactory;
    protected SqlParser.Config parserConfig;
    protected TddlValidator validator;
    protected CalciteCatalogReader catalog;
    protected SqlOperatorTable opTab;
    protected SqlToRelConverter.Config converterConfig;
    protected SqlToRelConverter sqlToRelConverter;

    static {
        try {
            nodeTypes = SqlValidatorImpl.class.getDeclaredField("nodeToTypeMap");
            nodeTypes.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        final MySqlCreateTableStatement stat1 = (MySqlCreateTableStatement) FastsqlUtils.parseSql(TABLE_DDL).get(0);
        final MySqlCreateTableStatement stat2 =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(COLLATION_TEST_DDL).get(0);
        final TableMeta tm1 = new TableMetaParser().parse(stat1, new ExecutionContext());
        final TableMeta tm2 = new TableMetaParser().parse(stat2, new ExecutionContext());
        tm1.setSchemaName(SCHEMA_NAME);
        tm2.setSchemaName(SCHEMA_NAME);
        schemaManager = new MySchemaManager();
        schemaManager.putTable("test", tm1);
        schemaManager.putTable("collation_test", tm2);

        typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        this.parserConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).setParserFactory(SqlParserImpl.FACTORY).build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);

        // prepare optimizer context
        OptimizerContext context = new OptimizerContext(SCHEMA_NAME);
        OptimizerContext.loadContext(context);
        context.setSchemaManager(schemaManager);

        String schemaName = OptimizerContext.getContext(SCHEMA_NAME).getSchemaName();
        CalciteSchema calciteSchema = RootSchemaFactory.createRootSchema(schemaName, new ExecutionContext());
        this.catalog = new CalciteCatalogReader(calciteSchema,
            calciteSchema.path(schemaName),
            new TddlJavaTypeFactoryImpl(),
            connectionConfig);

        this.opTab = TddlOperatorTable.instance();
        this.validator = new TddlValidator(opTab, catalog, typeFactory, SqlConformanceEnum.MYSQL_5);

        validator.setEnableTypeCoercion(true);
        validator.setTypeCoercion(new TypeCoercionImpl(typeFactory, validator));
        validator.setDefaultNullCollation(NullCollation.LOW);
        validator.setIdentifierExpansion(false);
        validator.setCallRewrite(false);

        this.converterConfig = SqlToRelConverter.configBuilder()
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
            new DrdsViewExpander(cluster, SqlConverter.getInstance(new ExecutionContext()), catalog),
            validator,
            catalog,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig,
            PlannerContext.getPlannerContext(cluster));
        StatisticManager.sds = MockStatisticDatasource.getInstance();
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    protected TypeCoercionTester sql(String sql) {
        return new TypeCoercionTester(sql);
    }

    protected TypeCoercionTester sql(String sql, boolean parameterized) {
        return new TypeCoercionTester(sql, parameterized);
    }

    protected class TypeCheckSqlVisitor implements SqlVisitor<String> {
        private List<SqlTypeName[]> castNodes;

        TypeCheckSqlVisitor(List<SqlTypeName[]> castNodes) {
            this.castNodes = castNodes;
        }

        private void handleSqlNode(SqlNode node) {
            if (castNodes != null) {
                return;
            }
            try {
                RelDataType type = validator.getValidatedNodeType(node);
                System.out.println(node.getClass().getSimpleName() + " | " + node + " -> " + type);
            } catch (Throwable t) {
                // discard this node.
            }
        }

        @Override
        public String visit(SqlLiteral literal) {
            return null;
        }

        @Override
        public String visit(SqlCall call) {
            if (castNodes != null && call.getOperator() == SqlStdOperatorTable.IMPLICIT_CAST) {
                SqlNode fromNode = call.getOperandList().get(0);
                SqlNode toNode = call.getOperandList().get(1);

                SqlTypeName fromType = fromNode instanceof SqlDynamicParam ?
                    ((SqlDynamicParam) fromNode).getTypeName()
                    : validator.getValidatedNodeType(fromNode).getSqlTypeName();
                SqlTypeName toType = toNode instanceof SqlDynamicParam ?
                    ((SqlDynamicParam) toNode).getTypeName()
                    : validator.getValidatedNodeType(toNode).getSqlTypeName();
                castNodes.add(new SqlTypeName[] {fromType, toType});
            }
            handleSqlNode(call);
            return call.getOperator().acceptCall(this, call);
        }

        @Override
        public String visit(SqlNodeList nodeList) {
            String result = null;
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                result = node.accept(this);
            }
            return result;
        }

        @Override
        public String visit(SqlIdentifier id) {
            handleSqlNode(id);
            return null;
        }

        @Override
        public String visit(SqlDataTypeSpec type) {
            handleSqlNode(type);
            return null;
        }

        @Override
        public String visit(SqlDynamicParam param) {
            handleSqlNode(param);
            return null;
        }

        @Override
        public String visit(SqlIntervalQualifier intervalQualifier) {
            handleSqlNode(intervalQualifier);
            return null;
        }
    }

    protected class MySchemaManager implements SchemaManager {

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

        public TableMeta getTable(String group, String tableName, String actralTableName) {
            return null;
        }

        @Override
        public void putTable(String tableName, TableMeta tableMeta) {
            tableMetaMap.put(tableName.toUpperCase(), tableMeta);
        }

        public Collection<TableMeta> getAllTables() {
            return tableMetaMap.values();
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
        public void init() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public boolean isInited() {
            return true;
        }

        @Override
        public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName,
                                                 EnumSet<IndexStatus> statusSet) {
            return GsiMetaManager.GsiMetaBean.empty();
        }

        @Override
        public String getSchemaName() {
            return "test";
        }
    }

    protected class TypeCoercionTester {
        private String sql;
        private SqlNode validatedAst;
        private RelDataType type;
        private List<SqlTypeName> sqlNodeTypeNameList;
        private List<SqlTypeName> relNodeTypeNameList;
        private List<SqlTypeName[]> castNodes;
        private RelNode relNode;
        private Throwable validateError;

        TypeCoercionTester(String sql) {
            this(sql, true);
        }

        TypeCoercionTester(String sql, boolean parameterized) {
            this.sql = sql;
            if (parameterized) {
                Map<Integer, ParameterContext> currentParameter = new HashMap<>();
                SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql, currentParameter, false);
                this.validatedAst = validate(sqlParameterized.getSql(), sqlParameterized.getParameters());
            } else {
                this.validatedAst = validate(sql, null);
            }
            if (validateError == null) {
                this.type = validator.getValidatedNodeType(validatedAst);
                if (type == null) {
                    throw new RuntimeException("invalid sql");
                }
                this.castNodes = new ArrayList<>();
                new TypeCheckSqlVisitor(castNodes).visit((SqlCall) validatedAst);
                this.sqlNodeTypeNameList =
                    type.getFieldList().stream().map((f) -> f.getType().getSqlTypeName()).collect(Collectors.toList());
                RelRoot relRoot = sqlToRelConverter.convertQuery(validatedAst, false, true);
                relNode = relRoot.rel;
                this.relNodeTypeNameList =
                    relNode.getRowType().getFieldList().stream().map((f) -> f.getType().getSqlTypeName())
                        .collect(Collectors.toList());
            }
        }

        private SqlNode validate(String sql, List<Object> params) {
            FastsqlParser fp = new FastsqlParser();
            SqlNodeList astList = params == null ? fp.parse(sql) : fp.parse(sql, params);
            SqlNode ast = astList.get(0);

            SqlNode validatedAst = null;
            try {
                validatedAst = validator.validate(ast);
            } catch (Throwable t) {
                validateError = t;
            }

            return validatedAst;
        }

        public TypeCoercionTester print() {
            System.out.println(type);
            return this;
        }

        public TypeCoercionTester checkDynamic(CharsetName charsetName, CollationName collationName) {
            validatedAst.accept(new SqlShuttle() {
                @Override
                public SqlNode visit(SqlDynamicParam param) {
                    SqlCollation sqlCollation = param.getCollation();
                    Charset charset = param.getCharset();
                    Assert.assertTrue(collationName.name().equalsIgnoreCase(sqlCollation.getCollationName()));
                    Assert.assertEquals(CharsetName.of(charset), charsetName);
                    return param;
                }
            });
            return this;
        }

        public TypeCoercionTester type(SqlTypeName... sqlTypeNames) {
            Preconditions.checkNotNull(sqlTypeNames);
            boolean equal = sqlNodeTypeNameList.equals(Arrays.asList(sqlTypeNames));
            Assert.assertTrue("the actual record type is " + sqlNodeTypeNameList.toString(), equal);
            return this;
        }

        public String type() {
            return sqlNodeTypeNameList.get(0).toString();
        }

        public TypeCoercionTester type(CharsetName charsetName, CollationName collationName) {
            type.getFieldList().stream()
                .map(RelDataTypeField::getType)
                .filter(SqlTypeUtil::isCharacter)
                .forEach(
                    t -> {
                        Preconditions.checkArgument(
                            t.getSqlTypeName() == SqlTypeName.VARCHAR || t.getSqlTypeName() == SqlTypeName.CHAR);
                        Assert.assertEquals(charsetName, CharsetName.of(t.getCharset()));
                        Assert.assertEquals(collationName, CollationName.of(t.getCollation().getCollationName()));
                    }
                );
            return this;
        }

        public ImmutablePair<CharsetName, CollationName> getCharsetAndCollation() throws Throwable {
            if (validateError != null) {
                throw validateError;
            }
            return type.getFieldList().stream()
                .map(RelDataTypeField::getType)
                .filter(SqlTypeUtil::isCharacter)
                .findFirst()
                .map(
                    t -> ImmutablePair
                        .of(CharsetName.of(t.getCharset()), CollationName.of(t.getCollation().getCollationName()))
                )
                .orElse(ImmutablePair.of(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI));
        }

        public TypeCoercionTester throwValidateError() {
            Assert.assertNotNull(validateError);
            return this;
        }

        public TypeCoercionTester noCast() {
            Assert.assertTrue("cast nodes are not empty and content is: " + printCastNodes(), castNodes.isEmpty());
            return this;
        }

        public TypeCoercionTester cast(SqlTypeName fromType, SqlTypeName toType) {
            Preconditions.checkNotNull(fromType);
            Preconditions.checkNotNull(toType);
            boolean exists = castNodes.stream().anyMatch((typeNames) ->
                typeNames[0] == fromType && typeNames[1] == toType
            );
            Assert.assertTrue("does not exist cast from " + fromType + " to " + toType +
                    ", the actual cast nodes is " + printCastNodes(),
                exists);
            return this;
        }

        public TypeCoercionTester noCast(SqlTypeName fromType, SqlTypeName toType) {
            Preconditions.checkNotNull(fromType);
            Preconditions.checkNotNull(toType);
            boolean exists = castNodes.stream().anyMatch((typeNames) ->
                typeNames[0] == fromType && typeNames[1] == toType
            );
            Assert.assertTrue("exist cast from " + fromType + " to " + toType, !exists);
            return this;
        }

        public TypeCoercionTester relType(SqlTypeName... sqlTypeNames) {
            Preconditions.checkNotNull(sqlTypeNames);
            boolean equal = relNodeTypeNameList.equals(Arrays.asList(sqlTypeNames));
            Assert.assertTrue("the actual rel record type is " + relNodeTypeNameList.toString(), equal);
            return this;
        }

        public SqlNode toSqlNode() {
            return validatedAst;
        }

        public RelNode toRelNode() {
            return relNode;
        }

        public void showTypes() {
            try {
                Map<SqlNode, RelDataType> nodeToTypeMap = (Map<SqlNode, RelDataType>) nodeTypes.get(validator);
                nodeToTypeMap.forEach((node, type) -> {
                    System.out.println(node + " : " + type);
                });
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        private String printCastNodes() {
            StringBuilder builder = new StringBuilder();
            for (SqlTypeName[] castNode : castNodes) {
                builder.append(castNode[0] + " -> " + castNode[1] + ", ");
            }
            return builder.toString();
        }
    }
}
