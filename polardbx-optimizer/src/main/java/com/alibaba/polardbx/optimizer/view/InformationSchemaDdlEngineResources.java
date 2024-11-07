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
 * Created by chenghui.lch
 *
 * @author chenghui.lch
 */
public class InformationSchemaDdlEngineResources extends VirtualView {

    protected InformationSchemaDdlEngineResources(RelOptCluster cluster,
                                                  RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.DDL_ENGINE_RESOURCE);
    }

    public InformationSchemaDdlEngineResources(RelInput input) {
        super(input);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int columnIndex = 0;
        columns.add(new RelDataTypeFieldImpl("HOST", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("RESOURCE_TYPE", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("TOTAL_AMOUNT", columnIndex++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("RESIDUE", columnIndex++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("ACTIVE_TASK", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("DETAIL", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        return typeFactory.createStructType(columns);
    }
}
