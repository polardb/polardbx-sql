package org.apache.calcite.sql;

import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @Author ShuGuang
 * @Description
 * @Date 2021/3/4 11:41 上午
 */
/**
 * @Author ShuGuang
 * @Description
 * @Date 2021/3/4 11:41 上午
 */
public class SqlChangeMaster extends SqlReplicationBase {

    {
        operator = new SqlChangeMasterOperator();
        sqlKind = SqlKind.CHANGE_MASTER;
        keyWord = "CHANGE MASTER TO";
    }

    public SqlChangeMaster(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options) {
        super(pos, options);
    }

    public SqlChangeMaster(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel) {
        super(pos, options, channel);
    }

    @Override
    protected void parseParams(String k, String v) {
        k = StringUtils.upperCase(k);
        switch (k) {
        case RplConstants.MASTER_HOST:
            params.put(RplConstants.MASTER_HOST, v);
            break;
        case RplConstants.MASTER_PORT:
            params.put(RplConstants.MASTER_PORT, v);
            break;
        case RplConstants.MASTER_USER:
            params.put(RplConstants.MASTER_USER, v);
            break;
        case RplConstants.MASTER_PASSWORD:
            params.put(RplConstants.MASTER_PASSWORD, v);
            break;
        case RplConstants.MASTER_LOG_FILE:
            params.put(RplConstants.MASTER_LOG_FILE, v);
            break;
        case RplConstants.MASTER_LOG_POS:
            params.put(RplConstants.MASTER_LOG_POS, v);
            break;
        case RplConstants.IGNORE_SERVER_IDS:
            params.put(RplConstants.IGNORE_SERVER_IDS, v);
            break;
        case RplConstants.SOURCE_HOST_TYPE:
            params.put(RplConstants.SOURCE_HOST_TYPE, v);
            break;
        case RplConstants.DEST_APPLIER_TYPE:
            params.put(RplConstants.DEST_APPLIER_TYPE, v);
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, String.format("Unrecognized arguments: %s", k));
        }
    }

    public static class SqlChangeMasterOperator extends SqlReplicationOperator {

        public SqlChangeMasterOperator() {
            super(SqlKind.CHANGE_MASTER);
        }
    }
}