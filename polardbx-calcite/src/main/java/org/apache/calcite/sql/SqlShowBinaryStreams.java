package org.apache.calcite.sql;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlShowBinaryStreams extends SqlShow {
    private static final List<SqlSpecialIdentifier> SPECIAL_IDENTIFIERS = Lists.newArrayList(
        SqlSpecialIdentifier.BINARY,
        SqlSpecialIdentifier.STREAMS);

    public SqlShowBinaryStreams(SqlParserPos pos) {
        super(pos, SPECIAL_IDENTIFIERS);
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_BINARY_STREAMS;
    }
}
