package org.apache.calcite.sql;

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
    private SqlNode ttlFilter = null;
    private SqlNode ttlCleanup = null;
    private SqlNode ttlPartInterval = null;
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
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MODIFY TTL ", "");

        int alterOptionCount = 0;
        if (ttlEnable != null) {
            alterOptionCount++;
            writer.print("TTL_ENABLE = ");
            writer.print(String.format("'%s'", ttlEnable));
        }

        if (ttlExpr != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("TTL_EXPR = ");
            ttlExpr.unparse(writer,leftPrec, rightPrec);
        }

        if (ttlJob != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("TTL_JOB = ");
            ttlJob.unparse(writer,leftPrec, rightPrec);
        }

        if (ttlFilter != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("TTL_FILTER = COND_EXPR(");
            ttlFilter.unparse(writer,leftPrec, rightPrec);
            writer.print(")");
        }

        if (ttlCleanup != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("TTL_CLEANUP = ");
            ttlCleanup.unparse(writer,leftPrec, rightPrec);
        }

        if (ttlPartInterval != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("TTL_PART_INTERVAL = ");
            ttlPartInterval.unparse(writer,leftPrec, rightPrec);
        }

        if (archiveTableSchema != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("ARCHIVE_TABLE_SCHEMA = ");
            writer.print(String.format("'%s'", archiveTableSchema));
        }

        if (archiveTableName != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("ARCHIVE_TABLE_NAME = ");
            writer.print(String.format("'%s'", archiveTableSchema));
        }

        if (archiveKind != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("ARCHIVE_TYPE = ");
            writer.print(String.format("'%s'", archiveKind));
        }

        if (arcPreAllocate != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("ARCHIVE_TABLE_PRE_ALLOCATE = ");
            writer.print(arcPreAllocate);
        }

        if (arcPostAllocate != null) {
            alterOptionCount++;
            if (alterOptionCount > 1) {
                writer.print(", ");
            }
            writer.print("ARCHIVE_TABLE_POST_ALLOCATE = ");
            writer.print(arcPostAllocate);
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

    public SqlNode getTtlFilter() {
        return ttlFilter;
    }

    public void setTtlFilter(SqlNode ttlFilter) {
        this.ttlFilter = ttlFilter;
    }

    public SqlNode getTtlCleanup() {
        return ttlCleanup;
    }

    public void setTtlCleanup(SqlNode ttlCleanup) {
        this.ttlCleanup = ttlCleanup;
    }

    public SqlNode getTtlPartInterval() {
        return ttlPartInterval;
    }

    public void setTtlPartInterval(SqlNode ttlPartInterval) {
        this.ttlPartInterval = ttlPartInterval;
    }

}