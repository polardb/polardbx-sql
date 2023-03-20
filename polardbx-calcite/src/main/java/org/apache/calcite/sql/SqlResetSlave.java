package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * @author shicai.xsc 2021/5/26 16:30
 * @since 5.0.0.0
 */
public class SqlResetSlave extends SqlReplicationBase {

    private boolean isAll;

    {
        operator = new SqlResetSlaveOperator();
        sqlKind = SqlKind.RESET_SLAVE;
        keyWord = "RESET SLAVE";
    }

    public SqlResetSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, boolean isAll) {
        super(pos, options);
        this.isAll = isAll;
    }

    public SqlResetSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel, boolean isAll) {
        super(pos, options, channel);
        this.isAll = isAll;
    }

    public static class SqlResetSlaveOperator extends SqlReplicationOperator {

        public SqlResetSlaveOperator() {
            super(SqlKind.RESET_SLAVE);
        }
    }

    public boolean isAll() {
        return isAll;
    }
}
