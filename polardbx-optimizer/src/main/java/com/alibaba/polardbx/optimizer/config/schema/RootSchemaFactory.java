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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author lingce.ldm 2017-07-06 20:16
 */
public class RootSchemaFactory {

    private static final Logger logger = LoggerFactory.getLogger(RootSchemaFactory.class);

    /**
     * 缓存所有的Schema,每个 APPNAME 对应一个
     */
    private static LoadingCache<SchemaManager, TddlSchema> schemaCache = CacheBuilder.newBuilder()
        .maximumSize(TddlConstants.DEFAULT_SCHEMA_CACHE_SIZE)
        .expireAfterWrite(300 * 1000, TimeUnit.MILLISECONDS)
        .softValues()
        .build(new CacheLoader<SchemaManager, TddlSchema>() {

            @Override
            public TddlSchema load(SchemaManager schemaManager)
                throws Exception {
                return new TddlSchema(schemaManager.getSchemaName(), schemaManager);
            }
        });

    public final static TddlSchema getSchema(SchemaManager schemaManager) {
        Preconditions.checkArgument(schemaManager != null);
        try {
            /**
             * 全部转化为小写
             */
            return schemaCache.get(schemaManager);
        } catch (ExecutionException e) {
            String errMsg = "Get schema error";
            logger.error("Get schema error.");
            throw new OptimizerException(e, errMsg);
        }
    }

    public static CalciteSchema createRootSchema(String schemaName, ExecutionContext ec) {
        return createRootSchema(schemaName, ec.getSchemaManagers());
    }

    public static CalciteSchema createRootSchema(String schemaName, Map<String, SchemaManager> schemaManagerMap) {
        final Schema schema = new RootSchema();
        final SchemaPlus rootSchema = new TddlCalciteSchema(schemaName, schemaManagerMap, null, schema, "").plus();
        SchemaManager schemaManager = null;
        if (schemaManagerMap != null && schemaManagerMap.containsKey(schemaName)) {
            schemaManager = schemaManagerMap.get(schemaName);
        } else {
            schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        }
        rootSchema.add(schemaName, getSchema(schemaManager));
        rootSchema.add(InformationSchema.NAME, InformationSchema.getInstance());
        rootSchema.add(PerformanceSchema.NAME, PerformanceSchema.getInstance());
        rootSchema.add(MysqlSchema.NAME, MysqlSchema.getInstance());
        rootSchema.add(MetaDbSchema.NAME, MetaDbSchema.getInstance());
        rootSchema.add(DefaultDbSchema.NAME, DefaultDbSchema.getInstance());
        rootSchema.add("dual", new DualTable());
        return CalciteSchema.from(rootSchema);
    }

    /**
     * Schema that has no parents.
     */
    static public class RootSchema extends AbstractSchema {

        public RootSchema() {
            super();
        }

        @Override
        public Expression getExpression(SchemaPlus parentSchema, String name) {
            return Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        }
    }
}
