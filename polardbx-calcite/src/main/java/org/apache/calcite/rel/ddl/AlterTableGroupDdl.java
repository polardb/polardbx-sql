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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;

/**
 * @author chenghui.lch
 */
public class AlterTableGroupDdl extends DDL {
    protected String tableGroupName;
    /**
     * When using the syntax of 'alter tablegroup by table',
     * the tableGroupName will be fixed as the real tg name
     */
    protected boolean fixedTgName = false;

    protected AlterTableGroupDdl(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                 RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public boolean isFixedTgName() {
        return fixedTgName;
    }

    public void setFixedTgName(boolean fixedTgName) {
        this.fixedTgName = fixedTgName;
    }
}
