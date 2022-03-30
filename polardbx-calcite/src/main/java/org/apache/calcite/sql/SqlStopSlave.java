package org.apache.calcite.sql;

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/**
 * @author shicai.xsc 2021/3/5 13:08
 * @desc
 * @since 5.0.0.0
 */
public class SqlStopSlave extends SqlReplicationBase {

    {
        operator = new SqlStopSlaveOperator();
        sqlKind = SqlKind.STOP_SLAVE;
        keyWord = "STOP SLAVE";
    }

    public SqlStopSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options){
        super(pos, options);
    }

    public SqlStopSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel){
        super(pos, options, channel);
    }

    public static class SqlStopSlaveOperator extends SqlReplicationOperator {

        public SqlStopSlaveOperator(){
            super(SqlKind.STOP_SLAVE);
        }
    }
}
