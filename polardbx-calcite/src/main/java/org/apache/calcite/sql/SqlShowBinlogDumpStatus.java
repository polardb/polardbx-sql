package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowBinlogDumpStatus extends SqlShow {
    private SqlNode with;

    public SqlShowBinlogDumpStatus(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, SqlNode with) {
        super(pos, specialIdentifiers);
        this.with = with;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_BINLOG_DUMP_STATUS;
    }

    public SqlNode getWith() {
        return with;
    }
}
