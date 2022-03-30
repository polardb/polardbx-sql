package org.apache.calcite.sql;


import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/**
 * @author shicai.xsc 2021/3/5 13:09
 * @desc
 * @since 5.0.0.0
 */
public class SqlShowSlaveStatus extends SqlReplicationBase {

    {
        operator = new SqlShowSlaveStatusOperator();
        sqlKind = SqlKind.SHOW_SLAVE_STATUS;
        keyWord = "SHOW SLAVE STATUS";
    }

    public SqlShowSlaveStatus(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options){
        super(pos, options);
    }

    public SqlShowSlaveStatus(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel){
        super(pos, options, channel);
    }

    public static class SqlShowSlaveStatusOperator extends SqlReplicationOperator {

        public SqlShowSlaveStatusOperator(){
            super(SqlKind.SHOW_SLAVE_STATUS);
        }
    }
}