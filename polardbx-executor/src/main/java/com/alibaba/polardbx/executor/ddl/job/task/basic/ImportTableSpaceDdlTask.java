package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.TddlConstants.LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN;

@Getter
@TaskName(name = "ImportTableSpaceDdlTask")
public class ImportTableSpaceDdlTask extends BaseDdlTask {

    private String logicalTableName;
    private String phyDbName;
    private String phyTableName;
    private Pair<String, Integer> targetHost;//leader、follower or leaner
    private Pair<String, String> userAndPasswd;

    @JSONCreator
    public ImportTableSpaceDdlTask(String schemaName, String logicalTableName, String phyDbName, String phyTableName,
                                   Pair<String, Integer> targetHost, Pair<String, String> userAndPasswd) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.phyDbName = phyDbName.toLowerCase();
        this.phyTableName = phyTableName.toLowerCase();
        this.targetHost = targetHost;
        this.userAndPasswd = userAndPasswd;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executeImpl(executionContext);
    }

    public void executeImpl(ExecutionContext executionContext) {
        ///!!!!!!DANGER!!!!
        // can't change variables via sql bypass CN
        // String disableBinlog = "SET SESSION sql_log_bin=0;
        HashMap<String, Object> variables = new HashMap<>();
        //disable sql_lon_bin
        variables.put(PhysicalBackfillUtils.SQL_LOG_BIN, "OFF");
        String importTableSpace = "alter table " + phyTableName + " import tablespace";
        try (
            XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(phyDbName,
                targetHost.getKey(), targetHost.getValue(), userAndPasswd.getKey(), userAndPasswd.getValue(), -1))) {
            try {
                conn.setLastException(new Exception("discard connection due to change SQL_LOG_BIN in this session"),
                    true);
                conn.setNetworkTimeoutNanos(LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN * 1000000L);
                //disable sql_lon_bin
                conn.setSessionVariables(variables);
                SQLRecorderLogger.ddlLogger.info(
                    String.format("begin to execute import tablespace command %s, in host: %s, db:%s", importTableSpace,
                        targetHost, phyDbName));
                conn.execQuery(importTableSpace);
                SQLRecorderLogger.ddlLogger.info(
                    String.format("finish execute import tablespace command %s, in host: %s, db:%s", importTableSpace,
                        targetHost, phyDbName));
            } finally {
                variables.clear();
                //reset
                conn.setSessionVariables(variables);
            }

        } catch (Exception ex) {
            try {
                if (tableSpaceExistError(ex.toString())) {
                    //pass
                } else {
                    throw ex;
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e, "import tablespace error");
            }
        }
    }

    private boolean tableSpaceExistError(String errMsg) {
        if (StringUtils.isEmpty(errMsg)) {
            return false;
        }
        String pattern = "tablespace.*exists";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(errMsg.toLowerCase());
        if (matcher.find()) {
            return true;
        } else {
            return false;
        }
    }

    public void rollbackImpl(ExecutionContext executionContext) {
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext ec) {
        rollbackImpl(ec);
    }

    @Override
    public String remark() {
        return "|alter table " + phyTableName + " import tablespace, phyDb:" + phyDbName + " host:" + targetHost;
    }

    public List<String> explainInfo() {
        String importTableSpace = "alter table " + phyTableName + " import tablespace";
        List<String> command = new ArrayList<>(1);
        command.add(importTableSpace);
        return command;
    }
}
