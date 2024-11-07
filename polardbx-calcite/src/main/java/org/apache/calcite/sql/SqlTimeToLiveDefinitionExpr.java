package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

/**
 * @author chenghui.lch
 */
public class SqlTimeToLiveDefinitionExpr extends SqlNode {

    protected SqlNode ttlEnableExpr;
    protected SqlNode ttlExpr;
    protected SqlNode ttlJobExpr;
    protected SqlNode ttlFilterExpr;
    protected SqlNode archiveTypeExpr;
    protected SqlNode archiveTableSchemaExpr;
    protected SqlNode archiveTableNameExpr;
    protected SqlNode archiveTablePreAllocateExpr;
    protected SqlNode archiveTablePostAllocateExpr;

    public SqlTimeToLiveDefinitionExpr() {
        super(SqlParserPos.ZERO);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

//        final SqlWriter.Frame startFrame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);

        writer.print("TTL_DEFINITION(");
        if (ttlEnableExpr != null) {
            writer.print("TTL_ENABLE = ");
            ttlEnableExpr.unparse(writer, leftPrec, rightPrec);
        }
        if (ttlExpr != null) {
            writer.print(", ");
            writer.print("TTL_EXPR = ");
            ttlExpr.unparse(writer, leftPrec, rightPrec);
        }
        if (ttlJobExpr != null) {
            writer.print(", ");
            writer.print("TTL_JOB = ");
            ttlJobExpr.unparse(writer, leftPrec, rightPrec);
        }
        if (ttlFilterExpr != null) {
            writer.print(", ");
            writer.print("TTL_FILTER = ");
            ttlFilterExpr.unparse(writer, leftPrec, rightPrec);
        }
        if (archiveTypeExpr != null) {
            writer.print(", ");
            writer.print("ARCHIVE_TYPE = ");
            archiveTypeExpr.unparse(writer, leftPrec, rightPrec);
        }

        if (archiveTableSchemaExpr != null) {
            writer.print(", ");
            writer.print("ARCHIVE_TABLE_SCHEMA = ");
            archiveTableSchemaExpr.unparse(writer, leftPrec, rightPrec);
        }

        if (archiveTableNameExpr != null) {
            writer.print(", ");
            writer.print("ARCHIVE_TABLE_NAME = ");
            archiveTableNameExpr.unparse(writer, leftPrec, rightPrec);
        }

        if (archiveTablePreAllocateExpr != null) {
            writer.print(", ");
            writer.print("ARCHIVE_TABLE_PRE_ALLOCATE = ");
            archiveTablePreAllocateExpr.unparse(writer, leftPrec, rightPrec);
        }

        if (archiveTablePostAllocateExpr != null) {
            writer.print(", ");
            writer.print("ARCHIVE_TABLE_POST_ALLOCATE = ");
            archiveTablePostAllocateExpr.unparse(writer, leftPrec, rightPrec);
        }

        writer.print(")");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        return false;
    }


    public SqlNode getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(SqlNode ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public SqlNode getTtlJobExpr() {
        return ttlJobExpr;
    }

    public void setTtlJobExpr(SqlNode ttlJobExpr) {
        this.ttlJobExpr = ttlJobExpr;
    }

    public SqlNode getTtlFilterExpr() {
        return ttlFilterExpr;
    }

    public void setTtlFilterExpr(SqlNode ttlFilterExpr) {
        this.ttlFilterExpr = ttlFilterExpr;
    }

    public SqlNode getArchiveTypeExpr() {
        return archiveTypeExpr;
    }

    public void setArchiveTypeExpr(SqlNode archiveTypeExpr) {
        this.archiveTypeExpr = archiveTypeExpr;
    }

    public SqlNode getArchiveTableSchemaExpr() {
        return archiveTableSchemaExpr;
    }

    public void setArchiveTableSchemaExpr(SqlNode archiveTableSchemaExpr) {
        this.archiveTableSchemaExpr = archiveTableSchemaExpr;
    }


    public SqlNode getArchiveTablePreAllocateExpr() {
        return archiveTablePreAllocateExpr;
    }

    public void setArchiveTablePreAllocateExpr(SqlNode archiveTablePreAllocateExpr) {
        this.archiveTablePreAllocateExpr = archiveTablePreAllocateExpr;
    }

    public SqlNode getArchiveTablePostAllocateExpr() {
        return archiveTablePostAllocateExpr;
    }

    public void setArchiveTablePostAllocateExpr(SqlNode archiveTablePostAllocateExpr) {
        this.archiveTablePostAllocateExpr = archiveTablePostAllocateExpr;
    }

    public SqlNode getTtlEnableExpr() {
        return ttlEnableExpr;
    }

    public void setTtlEnableExpr(SqlNode ttlEnableExpr) {
        this.ttlEnableExpr = ttlEnableExpr;
    }

    public SqlNode getArchiveTableNameExpr() {
        return archiveTableNameExpr;
    }

    public void setArchiveTableNameExpr(SqlNode archiveTableNameExpr) {
        this.archiveTableNameExpr = archiveTableNameExpr;
    }
}
