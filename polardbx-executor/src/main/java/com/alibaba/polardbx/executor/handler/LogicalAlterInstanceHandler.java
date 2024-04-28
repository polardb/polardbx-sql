package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.factory.LogicalAlterInstanceReadonlyStatusFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterInstance;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlAlterInstance;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.commons.lang.BooleanUtils;

import java.util.Map;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterInstanceHandler extends LogicalCommonDdlHandler {
    protected static final Map<String, Set<String>> supportedOptionAndValues
        = ImmutableMap.of(
        "read_only", ImmutableSet.of("false", "true")
    );

    public LogicalAlterInstanceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterInstance logicalAlterDatabase = (LogicalAlterInstance) logicalDdlPlan;

        SqlAlterInstance sqlAlterDatabase = (SqlAlterInstance) logicalAlterDatabase.relDdl.sqlNode;

        for (SqlSetOption option : sqlAlterDatabase.getOptitionList()) {
            String optionName = option.getName().getSimple().toLowerCase();
            String value = option.getValue().toString().toLowerCase();
            if (supportedOptionAndValues.containsKey(optionName) && (supportedOptionAndValues.get(optionName).isEmpty()
                || supportedOptionAndValues.get(optionName)
                .contains(value))) {
                if (optionName.equalsIgnoreCase("read_only")) {
                    boolean readonly = BooleanUtils.toBoolean(value);
                    return new LogicalAlterInstanceReadonlyStatusFactory(readonly).create();
                }
            }
        }

        return new TransientDdlJob();
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterInstance logicalAlterInstance = (LogicalAlterInstance) logicalDdlPlan;

        SqlAlterInstance sqlAlterInstance = (SqlAlterInstance) logicalAlterInstance.relDdl.sqlNode;

        for (SqlSetOption option : sqlAlterInstance.getOptitionList()) {
            String optionName = option.getName().getSimple().toLowerCase();
            String value = option.getValue().toString().toLowerCase();
            if (!supportedOptionAndValues.containsKey(optionName) || (!supportedOptionAndValues.get(optionName)
                .contains(value) && !supportedOptionAndValues.get(optionName).isEmpty())) {
                throw new TddlNestableRuntimeException(
                    String.format("option [%s=%s] is not supported", optionName, value)
                );
            }
        }
        return false;
    }
}
