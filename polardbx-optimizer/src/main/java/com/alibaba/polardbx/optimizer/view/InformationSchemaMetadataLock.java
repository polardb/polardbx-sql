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

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class InformationSchemaMetadataLock extends VirtualView {

    public InformationSchemaMetadataLock(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.METADATA_LOCK);
    }

    public InformationSchemaMetadataLock(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("NODE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CONN_ID", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TRX_ID", 2, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TRACE_ID", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SCHEMA", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TYPE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DURATION", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("VALIDATE", 8, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("FRONTEND", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SQL", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}
