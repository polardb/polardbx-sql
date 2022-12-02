package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlAlterTablePurgeBeforeTimeStamp extends SqlAlterTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlAlterTablePurgeBeforeTimeStamp.class);

    private final SqlIdentifier originTableName;

    private final SqlCharStringLiteral timestamp;

    public SqlAlterTablePurgeBeforeTimeStamp(SqlIdentifier tableName,
                                      String sql,
                                      SqlNode timestamp) {

        super(null, tableName, null, sql, null, new ArrayList<>(), SqlParserPos.ZERO);
        this.name = tableName;
        this.originTableName = tableName;
        if (!(timestamp instanceof SqlCharStringLiteral)) {
            throw new IllegalArgumentException("Timestamp format must be yyyy-mm-dd hh:mm:ss");
        }
        this.timestamp = (SqlCharStringLiteral) timestamp;
    }

    @Override
    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlCharStringLiteral getTimestamp() {
        return timestamp;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, false);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, boolean withOriginTableName) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ALTER TABLE", "");

        name.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    @Override
    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        return getSourceSql();
    }

    public String getSchemaName() {
        return originTableName.getComponent(0).getLastName();
    }

    public String getPrimaryTableName() {
        return originTableName.getComponent(1).getLastName();
    }

    @Override
    public SqlNode getTargetTable() {
        return super.getTargetTable();
    }

    @Override
    public String toString() {
        return prepare();
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }
}


