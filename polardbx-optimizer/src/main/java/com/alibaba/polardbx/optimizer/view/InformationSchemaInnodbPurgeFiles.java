package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

public class InformationSchemaInnodbPurgeFiles extends VirtualView {
    public InformationSchemaInnodbPurgeFiles(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.INNODB_PURGE_FILES);
    }

    public InformationSchemaInnodbPurgeFiles(RelInput relInput) {
        super(relInput.getCluster(), relInput.getTraitSet(),
            relInput.getEnum("virtualViewType", VirtualViewType.class));
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("log_id", 0, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("start_time", 1, typeFactory.createSqlType(SqlTypeName.DATETIME)));
        columns.add(new RelDataTypeFieldImpl("original_path", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns
            .add(new RelDataTypeFieldImpl("original_size", 3, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("temporary_path", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns
            .add(new RelDataTypeFieldImpl("current_size", 5, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));

        return typeFactory.createStructType(columns);
    }
}
