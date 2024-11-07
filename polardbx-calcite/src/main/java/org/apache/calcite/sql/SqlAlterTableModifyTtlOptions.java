package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**.
 *
 * @author chenghui.lch
 */
public class SqlAlterTableModifyTtlOptions extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("MODIFY TTL", SqlKind.MODIFY_TTL);

    private String ttlEnable = null;
    private SqlNode ttlExpr = null;
    private SqlNode ttlJob = null;
    private String archiveTableSchema = null;
    private String archiveTableName = null;
    private String archiveKind = null;
    private Integer arcPreAllocate = null;
    private Integer arcPostAllocate = null;

    public SqlAlterTableModifyTtlOptions(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> opList = new ArrayList<>();
        opList.add(ttlExpr);
        opList.add(ttlJob);
        return opList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MODIFY TTL", "");

        if (ttlEnable != null) {
            writer.print(" TTL_ENABLE = ");
            writer.print(String.format("'%s'", ttlEnable));
            writer.print(" ");
        }

        if (ttlExpr != null) {
            writer.print(" TTL_EXPR = ");
            ttlExpr.unparse(writer,leftPrec, rightPrec);
            writer.print(" ");
        }

        if (ttlJob != null) {
            writer.print(" TTL_JOB = ");
            ttlJob.unparse(writer,leftPrec, rightPrec);
            writer.print(" ");
        }

        if (archiveTableSchema != null) {
            writer.print(" ARCHIVE_TABLE_SCHEMA = ");
            writer.print(String.format("'%s'", archiveTableSchema));
            writer.print(" ");
        }

        if (archiveTableName != null) {
            writer.print(" ARCHIVE_TABLE_NAME = ");
            writer.print(String.format("'%s'", archiveTableSchema));
            writer.print(" ");
        }

        if (archiveKind != null) {
            writer.print(" ARCHIVE_KIND = ");
            writer.print(String.format("'%s'", archiveKind));
            writer.print(" ");
        }

        if (arcPreAllocate != null) {
            writer.print(" ARC_PRE_ALLOCATE = ");
            writer.print(arcPreAllocate);
            writer.print(" ");
        }

        if (arcPostAllocate != null) {
            writer.print(" ARC_POST_ALLOCATE = ");
            writer.print(arcPostAllocate);
            writer.print(" ");
        }

        writer.endList(frame);
    }

    public String getTtlEnable() {
        return ttlEnable;
    }

    public void setTtlEnable(String ttlEnable) {
        this.ttlEnable = ttlEnable;
    }

    public SqlNode getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(SqlNode ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public SqlNode getTtlJob() {
        return ttlJob;
    }

    public void setTtlJob(SqlNode ttlJob) {
        this.ttlJob = ttlJob;
    }

    public String getArchiveTableName() {
        return archiveTableName;
    }

    public void setArchiveTableName(String archiveTableName) {
        this.archiveTableName = archiveTableName;
    }

    public String getArchiveTableSchema() {
        return archiveTableSchema;
    }

    public void setArchiveTableSchema(String archiveTableSchema) {
        this.archiveTableSchema = archiveTableSchema;
    }

    public String getArchiveKind() {
        return archiveKind;
    }

    public void setArchiveKind(String archiveKind) {
        this.archiveKind = archiveKind;
    }

    public Integer getArcPreAllocate() {
        return arcPreAllocate;
    }

    public void setArcPreAllocate(Integer arcPreAllocate) {
        this.arcPreAllocate = arcPreAllocate;
    }

    public Integer getArcPostAllocate() {
        return arcPostAllocate;
    }

    public void setArcPostAllocate(Integer arcPostAllocate) {
        this.arcPostAllocate = arcPostAllocate;
    }

}