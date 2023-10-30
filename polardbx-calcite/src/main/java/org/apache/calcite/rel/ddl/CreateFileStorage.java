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

package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

public class CreateFileStorage extends DDL {
    private boolean ifNotExists;

    private final String engineName;
    private final Map<String, String> with;

    protected CreateFileStorage(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                              RelDataType rowType, String engineName, Map<String, String> with, boolean ifNotExists) {
        super(cluster, traits, ddl, rowType);
        this.engineName = engineName;
        this.with = with;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(engineName, SqlParserPos.ZERO));
        this.ifNotExists = ifNotExists;
    }

    public static CreateFileStorage create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                           RelDataType rowType,  String engineName, Map<String, String> with,
                                           boolean ifNotExists) {

        return new CreateFileStorage(cluster, traits, ddl, rowType, engineName, with, ifNotExists);
    }

    @Override
    public CreateFileStorage copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new CreateFileStorage(this.getCluster(), traitSet, this.ddl, rowType, engineName, with, ifNotExists);
    }

    public String getEngineName() {
        return engineName;
    }

    public Map<String, String> getWith() {
        return with;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }
}
