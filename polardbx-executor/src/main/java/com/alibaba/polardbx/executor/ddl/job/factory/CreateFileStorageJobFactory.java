/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        if (engine.name().equalsIgnoreCase("OSS")) {
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
