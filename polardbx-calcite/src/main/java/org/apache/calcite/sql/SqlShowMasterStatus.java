package org.apache.calcite.sql;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class SqlShowMasterStatus extends SqlShow {
    private SqlNode with;
    private static final List<SqlNode> OPERANDS_EMPTY = new ArrayList<>(0);
    private static final List<SqlSpecialIdentifier> SPECIAL_IDENTIFIERS = Lists.newArrayList(
        SqlSpecialIdentifier.MASTER,
        SqlSpecialIdentifier.STATUS);

    public SqlShowMasterStatus(SqlParserPos pos) {
        super(pos, SPECIAL_IDENTIFIERS);
    }

    public SqlShowMasterStatus(SqlParserPos pos, SqlNode with) {
        super(pos, SPECIAL_IDENTIFIERS);
        this.with = with;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_MASTER_STATUS;
    }

    public SqlNode getWith() {
        return with;
    }
}
