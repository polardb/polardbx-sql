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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

/**
 * Created by zhuqiwei.
 */
public class LogicalTableDataMigrationBackfill extends AbstractRelNode {

    private String srcSchemaName;
    private String dstSchemaName;
    private String srcTableName;
    private String dstTableName;

    public LogicalTableDataMigrationBackfill(RelOptCluster cluster, RelTraitSet traitSet, String srcSchemaName,
                                             String dstSchemaName,
                                             String srcTableName, String dstTableName) {
        super(cluster, traitSet);
        this.srcSchemaName = srcSchemaName;
        this.dstSchemaName = dstSchemaName;
        this.srcTableName = srcTableName;
        this.dstTableName = dstTableName;
    }

    public static LogicalTableDataMigrationBackfill createLogicalTableDataMigrationBackfill(String srcSchemaName,
                                                                                            String dstSchemaName,
                                                                                            String srcTableName,
                                                                                            String dstTableName,
                                                                                            ExecutionContext ec) {
        final RelOptCluster cluster = SqlConverter.getInstance(srcSchemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new LogicalTableDataMigrationBackfill(cluster, traitSet, srcSchemaName, dstSchemaName, srcTableName,
            dstTableName);
    }

    public String getSrcSchemaName() {
        return srcSchemaName;
    }

    public void setSrcSchemaName(String srcSchemaName) {
        this.srcSchemaName = srcSchemaName;
    }

    public String getDstSchemaName() {
        return dstSchemaName;
    }

    public void setDstSchemaName(String dstSchemaName) {
        this.dstSchemaName = dstSchemaName;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public void setSrcTableName(String srcTableName) {
        this.srcTableName = srcTableName;
    }

    public String getDstTableName() {
        return dstTableName;
    }

    public void setDstTableName(String dstTableName) {
        this.dstTableName = dstTableName;
    }

}
