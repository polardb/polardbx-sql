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

package com.alibaba.polardbx.executor.mpp.client;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.RelNode;

public class MppRunner {

    private static final Logger log = LoggerFactory.getLogger(MppRunner.class);

    private RelNode relNode;
    private ExecutionContext executionContext;

    private LocalStatementClient client;

    public MppRunner(RelNode relNode, ExecutionContext executionContext) {
        this.relNode = relNode;
        this.executionContext = executionContext;
    }

    public Cursor execute() {

        if (!ServiceProvider.getInstance().clusterMode()) {
            // It is not ready for mpp
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, "Don't Start Up The MPP Server Now!");
        }
        try {
            Server server = ServiceProvider.getInstance().getServer();
            client = ((MppServer) server).newLocalStatementClient(executionContext, relNode);
            client.createQuery();
            CursorMeta cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(relNode, "Last"));
            return new MppResultCursor(client, executionContext, cursorMeta);
        } catch (Throwable t) {
            closeClient(t);
            log.error("run Mpp error:", t);
            throw GeneralUtil.nestedException(t);
        }
    }

    public void closeClient(Throwable t) {
        try {
            if (client != null) {
                client.failQuery(t);
            }
        } catch (Throwable ignore) {
            log.warn("error!", ignore);
        }
    }
}
