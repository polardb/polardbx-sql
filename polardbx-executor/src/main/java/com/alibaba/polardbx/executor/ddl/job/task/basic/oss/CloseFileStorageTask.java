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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.engine.FileStorageInfoAccessor;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CloseFileStorageTask")
public class CloseFileStorageTask extends BaseDdlTask {

    private String engine;
    private boolean onlyCloseColdData;

    @JSONCreator
    public CloseFileStorageTask(String engine, boolean onlyCloseColdData) {
        super(DefaultDbSchema.NAME);
        this.engine = engine;
        this.onlyCloseColdData = onlyCloseColdData;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        Engine fileEngine = Engine.of(engine);
        CommonMetaChanger.invalidateBufferPool();

        if (!onlyCloseColdData) {
            FileStorageInfoAccessor fileStorageInfoAccessor = new FileStorageInfoAccessor();
            fileStorageInfoAccessor.setConnection(metaDbConnection);
            fileStorageInfoAccessor.delete(fileEngine);
        }

        FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(fileEngine);
        fileStorageMetaStore.setConnection(metaDbConnection);
        fileStorageMetaStore.deleteAll();

        if (!onlyCloseColdData) {
            ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
            configListenerAccessor.setConnection(metaDbConnection);
            configListenerAccessor.updateOpVersion(MetaDbDataIdBuilder.getFileStorageInfoDataId());
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }
}
