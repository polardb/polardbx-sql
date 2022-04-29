package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateFileStorageTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.engine.FileStorageInfoKey;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.eclipse.jetty.util.StringUtil;

import java.util.Map;
import java.util.Set;

public class CreateFileStorageJobFactory extends DdlJobFactory {
    private static final Logger logger = LoggerFactory.getLogger("oss");

    private ExecutionContext executionContext;
    private Engine engine;
    private Map<FileStorageInfoKey, String> items;

    public CreateFileStorageJobFactory(
        Engine engine, Map<FileStorageInfoKey, String> items,
        ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.engine = engine;
        this.items = items;
    }

    @Override
    protected void validate() {
        if (!items.containsKey(FileStorageInfoKey.FILE_URI)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, "Should contain FILE_URI in with!");
        }
        if (StringUtil.endsWithIgnoreCase(engine.name(), "OSS")) {
            if (!items.containsKey(FileStorageInfoKey.ENDPOINT)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, "Should contain ENDPOINT in with!");
            }

            // check endpoint
            String endpointValue = items.get(FileStorageInfoKey.ENDPOINT);
            if (!OSSTaskUtils.checkEndpoint(endpointValue)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, "bad ENDPOINT value in with!");
            }

            if (!items.containsKey(FileStorageInfoKey.ACCESS_KEY_ID)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, "Should contain ACCESS_KEY_ID in with!");
            }
            if (!items.containsKey(FileStorageInfoKey.ACCESS_KEY_SECRET)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS,
                    "Should contain ACCESS_KEY_SECRET in with!");
            }
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addTask(new CreateFileStorageTask(executionContext.getSchemaName(), engine.name(), items));
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}
