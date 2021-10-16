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
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableGroup extends VirtualView {

    protected InformationSchemaTableGroup(RelOptCluster cluster,
                                          RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.TABLE_GROUP);
    }

    public InformationSchemaTableGroup(RelInput input) {
        super(input);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("SCHEMA_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE_GROUP_ID", 1, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TABLE_GROUP_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOCALITY", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PRIMARY_ZONE", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("IS_MANUAL_CREATE", 5, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("PART_INFO", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLES", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}
