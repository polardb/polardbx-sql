package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowBinlogEvents extends SqlShow {

    private SqlNode with, logName, pos, limit;

    public SqlShowBinlogEvents(SqlParserPos parserPos,
                               List<SqlSpecialIdentifier> specialIdentifiers,
                               List<SqlNode> operands, SqlNode logName, SqlNode pos, SqlNode limit) {
        super(parserPos, specialIdentifiers, operands);
        this.logName = logName;
        this.pos = pos;
        this.limit = limit;
    }

    public SqlShowBinlogEvents(SqlParserPos parserPos,
                               List<SqlSpecialIdentifier> specialIdentifiers,
                               List<SqlNode> operands, SqlNode with, SqlNode logName, SqlNode pos, SqlNode limit) {
        super(parserPos, specialIdentifiers, operands);
        this.with = with;
        this.logName = logName;
        this.pos = pos;
        this.limit = limit;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_BINLOG_EVENTS;
    }

    public SqlNode getWith() {
        return with;
    }

    public SqlNode getLogName() {
        return logName;
    }

    public SqlNode getPos() {
        return pos;
    }

    public SqlNode getLimit() {
        return limit;
    }
}
