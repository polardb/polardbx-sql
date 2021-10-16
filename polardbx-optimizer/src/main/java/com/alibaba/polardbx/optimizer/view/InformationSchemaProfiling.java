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

package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

public class InformationSchemaProfiling extends VirtualView {

    public InformationSchemaProfiling(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.PROFILING);
    }

    public InformationSchemaProfiling(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("QUERY_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("SEQ", 1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("STATE", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DURATION", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CPU_USER", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CPU_SYSTEM", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CONTEXT_VOLUNTARY", 6, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("CONTEXT_INVOLUNTARY", 7, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("BLOCK_OPS_IN", 8, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("BLOCK_OPS_OUT", 9, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("MESSAGES_SENT", 10, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("MESSAGES_RECEIVED", 11, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("PAGE_FAULTS_MAJOR", 12, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("PAGE_FAULTS_MINOR", 13, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("SWAPS", 14, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("SOURCE_FUNCTION", 15, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SOURCE_FILE", 16, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SOURCE_LINE", 17, typeFactory.createSqlType(SqlTypeName.INTEGER)));

        return typeFactory.createStructType(columns);
    }
}
