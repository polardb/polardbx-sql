package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wumu
 */
public class SqlResumeRebalanceJob extends SqlDal implements DdlJobUtility {

    private static final SqlSpecialOperator OPERATOR = new SqlResumeRebalanceJobOperator();
    private boolean all;
    private List<Long> jobIds = new ArrayList<>();

    public SqlResumeRebalanceJob(SqlParserPos pos, boolean all) {
        super(pos);
        this.operands = new ArrayList<>(0);
        this.all = all;
    }

    @Override
    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    @Override
    public List<Long> getJobIds() {
        return jobIds;
    }

    public void setJobIds(List<Long> jobIds) {
        this.jobIds.clear();
        this.jobIds.addAll(jobIds);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("RESUME REBALANCE");
        if (isAll()) {
            writer.print("ALL");
        } else {
            writer.print(StringUtils.join(jobIds, ","));
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.RESUME_REBALANCE_JOB;
    }

    public static class SqlResumeRebalanceJobOperator extends SqlSpecialOperator {

        public SqlResumeRebalanceJobOperator() {
            super("RESUME_REBALANCE_JOB", SqlKind.RESUME_REBALANCE_JOB);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("JOB_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("STATUS", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

}
