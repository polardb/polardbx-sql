package org.apache.calcite.sql;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableSplitPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SPLIT PARTITION", SqlKind.SPLIT_PARTITION);
    private final SqlNode splitPartitionName;
    private final SqlNode atValue;
    private final List<SqlPartition> newPartitions;

    public SqlAlterTableSplitPartition(SqlParserPos pos, SqlNode splitPartitionName, SqlNode atValue,
                                       List<SqlPartition> newPartitions) {
        super(pos);
        this.splitPartitionName = splitPartitionName;
        this.atValue = atValue;
        this.newPartitions = newPartitions == null ? new ArrayList<>() : newPartitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getAtValue() {
        return atValue;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public SqlNode getSplitPartitionName() {
        return splitPartitionName;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);

        if (GeneralUtil.isNotEmpty(newPartitions)) {

            List<SqlNode> partDefs = new ArrayList<>();
            partDefs.addAll(newPartitions);
            int partColCnt = -1;
            SqlPartitionBy.validatePartitionDefs(validator, scope, partDefs, partColCnt, true);
            if (this.atValue != null) {
                SqlNode v = this.atValue;
                if (v instanceof SqlIdentifier) {
                    String str = ((SqlIdentifier) v).getLastName();
                    if (str != null && str.toLowerCase().contains("maxvalue")) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                            "The at value is invalid"));
                    }
                }
                RelDataType dataType = validator.deriveType(scope, this.atValue);
                if (dataType == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The at value is invalid"));
                } else {
                    SqlTypeName typeName = dataType.getSqlTypeName();
                    if (!SqlTypeName.INT_TYPES.contains(typeName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                            "The at value must be an integer"));
                    }
                }
            }

        }
    }
}
