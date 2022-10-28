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

package com.alibaba.polardbx.qatest.dql.sharding.local;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.schema.RootSchemaFactory;
import com.alibaba.polardbx.optimizer.config.schema.TddlCalciteSchema;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlValidator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Type system basic class
 *
 * @author hongxi.chx
 */
abstract class BaseSqlTypeTest {
    protected RelRoot getRelRoot(String sql) {
        FastsqlParser fp = new FastsqlParser();
        SqlNodeList astList = fp.parse(sql);
        TddlTypeFactoryImpl typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        SqlParser.Config parserConfig =
            SqlParser.configBuilder().setLex(Lex.MYSQL).setParserFactory(SqlParserImpl.FACTORY).build();
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .build();
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        String schemaName = "test";
        SchemaPlus rootSchema =
            new TddlCalciteSchema(schemaName, null, null,
                new RootSchemaFactory.RootSchema(), "mock").plus();
        CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
        final CalciteCatalogReader catalog = new CalciteCatalogReader(calciteSchema,
            calciteSchema.path(schemaName),
            new TddlJavaTypeFactoryImpl(),
            connectionConfig);
        TddlOperatorTable opTab = TddlOperatorTable.instance();
        TddlValidator validator = new TddlValidator(opTab, catalog, typeFactory, SqlConformanceEnum.MYSQL_5);
        final SqlNode validate = validator.validate(astList.get(0));
//        System.out.println(validate);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCostFactory costFactory = DrdsRelOptCostImpl.FACTORY;
        RelOptPlanner planner = new VolcanoPlanner(DrdsRelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptCluster relOptCluster = RelOptCluster.create(planner, rexBuilder);
        relOptCluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(null,
            validator,
            catalog,
            relOptCluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);
        return sqlToRelConverter.convertQuery(validate, false, true);
    }

    @Test
    public void testNone() {

    }
}
