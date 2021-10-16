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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public abstract class AbsSchema implements Schema {

    protected static final Logger logger = LoggerFactory.getLogger(AbsSchema.class);

    protected AbsSchema() {
    }

    @Override
    public final boolean isMutable() {
        return true;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return null;
    }

    @Override
    public Schema getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return Collections.emptySet();
    }

    @Override
    public final Collection<Function> getFunctions(String name) {
        return Collections.emptyList();
    }

    @Override
    public final Set<String> getFunctionNames() {
        return Collections.emptySet();
    }

    @Override
    public final Expression getExpression(SchemaPlus parentSchema, String name) {
        return Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    }
}
