package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SqlAlterTableOptimizePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("OPTIMIZE PARTITION", SqlKind.OPTIMIZE_PARTITION);

    protected final List<SqlNode> partitions;

    protected SqlNode parent;
    protected boolean isSubPartition;

    public SqlAlterTableOptimizePartition(SqlParserPos pos, List<SqlNode> partitions, boolean isSubPartition) {
        super(pos);
        this.partitions = partitions;
        this.isSubPartition = isSubPartition;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return this.partitions;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "OPTIMIZE", "");

        writer.keyword("PARTITION");
        final SqlWriter.Frame partFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "(", ")");
        int i = 0;
        for (SqlNode sqlNode : partitions) {
            sqlNode.unparse(writer, leftPrec, rightPrec);
            i++;
            if (i < partitions.size()) {
                writer.sep(",");
            }
        }
        writer.endList(partFrame);

        writer.endList(frame);
    }

    public List<SqlNode> getPartitions() {
        return partitions;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        if (GeneralUtil.isNotEmpty(partitions)) {
            List<SqlNode> partDefs = new ArrayList<>();
            partDefs.addAll(partitions);
            int partColCnt = -1;
            SqlPartitionBy.validatePartitionDefs(validator, scope, partDefs, partColCnt, -1, true, false);
        }
    }

    public boolean isSubPartition() {
        return isSubPartition;
    }

    public void setSubPartition(boolean subPartition) {
        isSubPartition = subPartition;
    }
}
