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

package com.alibaba.polardbx.optimizer.hint;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.schema.RootSchemaFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.Properties;

/**
 * @author chenmo.cm
 */
public class HintPlanner4Test extends HintPlanner {

    public HintPlanner4Test(SqlValidatorImpl validator, CalciteCatalogReader catalog,
                            RelOptCluster cluster, Config converterConfig) {
        super(validator,
            catalog,
            cluster,
            converterConfig);
    }

    public static HintPlanner4Test getInstance(String schemaName) {
        CalciteCatalogReader catalog;
        TddlTypeFactoryImpl typeFactory;
        RelOptCluster cluster;
        Config converterConfig;

        typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        SqlParser.Config parserConfig = SqlParser.configBuilder()
            .setLex(Lex.MYSQL)
            .setParserFactory(SqlParserImpl.FACTORY)
            .build();
        converterConfig = SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        CalciteSchema calciteSchema = RootSchemaFactory.createRootSchema(schemaName, new ExecutionContext());
        catalog = new CalciteCatalogReader(calciteSchema,
            calciteSchema.path(schemaName),
            new TddlJavaTypeFactoryImpl(),
            connectionConfig);

        RelOptPlanner planner = new VolcanoPlanner(DrdsRelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);

        TddlOperatorTable opTab = TddlOperatorTable.instance();
        TddlValidator validator = new TddlValidator(opTab, catalog, typeFactory, SqlConformanceEnum.DEFAULT);
        validator.setDefaultNullCollation(NullCollation.LOW);
        validator.setIdentifierExpansion(false);
        validator.setCallRewrite(false);

        return new HintPlanner4Test(validator, catalog, cluster, converterConfig);
    }

    @Override
    public ExecutionPlan getOriginLogicalPlan(SqlNode ast, PlannerContext plannerContext) {
        return Planner.getInstance().getPlan(ast, plannerContext);
    }
}
