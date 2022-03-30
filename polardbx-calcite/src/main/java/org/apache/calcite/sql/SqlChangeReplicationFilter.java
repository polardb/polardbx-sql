package org.apache.calcite.sql;

import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.List;



/**
 * @author shicai.xsc 2021/3/5 13:09
 * @desc
 * @since 5.0.0.0
 */
public class SqlChangeReplicationFilter extends SqlReplicationBase {

    {
        operator = new SqlChangeReplicationFilterOperator();
        sqlKind = SqlKind.CHANGE_REPLICATION_FILTER;
        keyWord = "CHANGE REPLICATION FILTER";
    }

    public SqlChangeReplicationFilter(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options){
        super(pos, options);
    }

    @Override
    protected void parseParams(String k, String v) {
        k = StringUtils.upperCase(k);
        switch (k) {
        case RplConstants.REPLICATE_DO_DB:
            params.put(RplConstants.REPLICATE_DO_DB, v);
            break;
        case RplConstants.REPLICATE_IGNORE_DB:
            params.put(RplConstants.REPLICATE_IGNORE_DB, v);
            break;
        case RplConstants.REPLICATE_DO_TABLE:
            params.put(RplConstants.REPLICATE_DO_TABLE, v);
            break;
        case RplConstants.REPLICATE_IGNORE_TABLE:
            params.put(RplConstants.REPLICATE_IGNORE_TABLE, v);
            break;
        case RplConstants.REPLICATE_WILD_DO_TABLE:
            params.put(RplConstants.REPLICATE_WILD_DO_TABLE, v);
            break;
        case RplConstants.REPLICATE_WILD_IGNORE_TABLE:
            params.put(RplConstants.REPLICATE_WILD_IGNORE_TABLE, v);
            break;
        case RplConstants.REPLICATE_REWRITE_DB:
            params.put(RplConstants.REPLICATE_REWRITE_DB, v);
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, String.format("Unrecognized arguments: %s", k));
        }
    }

    public static class SqlChangeReplicationFilterOperator extends SqlReplicationOperator {

        public SqlChangeReplicationFilterOperator(){
            super(SqlKind.CHANGE_REPLICATION_FILTER);
        }
    }
}
