package org.apache.calcite.sql;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableSplitPartitionByHotValue extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SPLIT HOT VALUE", SqlKind.SPLIT_HOT_VALUE);
    private final List<SqlNode> hotKeys;
    private final SqlNode partitions;
    private final SQLName hotKeyPartitionName;

    public SqlAlterTableSplitPartitionByHotValue(SqlParserPos pos, List<SqlNode> hotkeys, SqlNode partitions,
                                                 SQLName hotKeyPartitionName) {
        super(pos);
        this.hotKeys = hotkeys;
        this.partitions = partitions;
        this.hotKeyPartitionName = hotKeyPartitionName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public List<SqlNode> getHotKeys() {
        return hotKeys;
    }

    public SqlNode getPartitions() {
        return partitions;
    }

    public SQLName getHotKeyPartitionName() {
        return hotKeyPartitionName;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        // Validate hotkeys
        for (SqlNode hotKey : hotKeys) {
            if (hotKey != null) {
                RelDataType dataType = validator.deriveType(scope, hotKey);
                if (dataType == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                        "The hot value is invalid"));
                }
            }
        }
        if (partitions != null) {
            RelDataType dataType = validator.deriveType(scope, partitions);
            if (dataType == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, String.format(
                    "The partitions number is invalid"));
            }
        }
    }
}