package org.apache.calcite.sql;

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/**
 * @author shicai.xsc 2021/3/5 13:08
 * @desc
 * @since 5.0.0.0
 */
public class SqlStartSlave extends SqlReplicationBase {

    {
        operator = new SqlStartSlaveOperator();
        sqlKind = SqlKind.START_SLAVE;
        keyWord = "START SLAVE";
    }

    public SqlStartSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options){
        super(pos, options);
    }

    public SqlStartSlave(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel){
        super(pos, options, channel);
    }

    public static class SqlStartSlaveOperator extends SqlReplicationOperator {

        public SqlStartSlaveOperator(){
            super(SqlKind.START_SLAVE);
        }
    }
}