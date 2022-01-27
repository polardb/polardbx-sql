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

package com.alibaba.polardbx.executor.mpp.deploy;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.client.LocalStatementClient;
import com.alibaba.polardbx.executor.mpp.discover.PolarDBXNodeStatusManager;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.HttpEventModule;
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.apache.calcite.rel.RelNode;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.properties.PropUtil.getCluster;
import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_POLARDBX;

public class MppServer extends Server {

    private volatile StatementResource statementResource;
    private String serverHost;
    private TaskManager taskManager;

    private boolean isMppServer;
    private boolean isMppWorker;

    protected final Map<String, String> bootstrapProperties = new HashMap<>();

    public MppServer(int id, boolean isMppServer, boolean isMppWorker, String serverHost, int mppHttpPort) {
        super(id, mppHttpPort);
        log.warn("MppServer nodeId=" + id + ",isMppServer=" + isMppServer + ",isMppWorker=" + isMppWorker
            + ",serverHost=" + serverHost + ",mppHttpPort=" + mppHttpPort);
        this.isMppWorker = isMppWorker;
        this.serverHost = serverHost;
        this.isMppServer = isMppServer;

        bootstrapProperties.put(BootstrapConfig.CONFIG_KEY_NODE_ID, this.nodeId);
        bootstrapProperties.put(BootstrapConfig.CONFIG_KEY_HTTP_PORT, String.valueOf(this.mppPort));
        bootstrapProperties.put(BootstrapConfig.CONFIG_KEY_NODE_ENV, MPP_POLARDBX);
        bootstrapProperties.put(BootstrapConfig.CONFIG_KEY_HTTP_SERVER_LOG_ENABLED, "false");
        bootstrapProperties.put(BootstrapConfig.CONFIG_KEY_HTTP_SERVER_MAX_THREADS,
            String.valueOf(MppConfig.getInstance().getHttpServerMaxThreads()));
        bootstrapProperties.put(BootstrapConfig.CONFIG_KEY_HTTP_SERVER_MIN_THREADS,
            String.valueOf(MppConfig.getInstance().getHttpServerMinThreads()));
        boolean htap = true;
        GmsNodeManager.GmsNode gmsNode = GmsNodeManager.getInstance().getLocalNode();
        htap = ConfigDataMode.isMasterMode() || gmsNode == null
            || gmsNode.instType == ServerInfoRecord.INST_TYPE_HTAP_SLAVE;
        String instId = InstIdUtil.getInstId();
        this.localNode = new InternalNode(
            nodeId, getCluster(MppConfig.getInstance().getDefaultCluster()), instId, serverHost,
            TddlNode.getPort(), mppPort,
            new NodeVersion(Version.getVersion()), isMppServer, isMppWorker, false, htap);
        if (ConfigDataMode.isMasterMode()) {
            localNode.setMaster(true);
        }
    }

    @Override
    public void run() throws TddlRuntimeException {
        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(new NodeModule());
        modules.add(new HttpServerModule());
        modules.add(new DiscoveryModule());
        modules.add(new JsonModule());
        modules.add(new JaxrsModule());
        modules.add(new HttpEventModule());
        localNode.setLeader(false);
        modules.add(new MainModule(localNode));
        if (isMppServer) {
            modules.add(new ServerModule());
        }
        if (isMppWorker) {
            modules.add(new WorkerModule());
        }

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector;
        try {
            injector = app.strictConfig().setRequiredConfigurationProperties(bootstrapProperties).initialize();
            injector.getInstance(HttpServer.class).start();
            if (isMppServer) {
                statementResource = injector.getInstance(StatementResource.class);
                queryManager = injector.getInstance(QueryManager.class);
            }
            if (isMppWorker) {
                taskManager = injector.getInstance(TaskManager.class);
            }
            nodeManager = injector.getInstance(InternalNodeManager.class);
            taskExecutor = injector.getInstance(TaskExecutor.class);
            spillerFactory = injector.getInstance(SpillerFactory.class);

            this.manager = new PolarDBXNodeStatusManager(nodeManager, localNode);
        } catch (Throwable t) {
            log.error("MppServer start error.", t);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, t, "MppServer start error");
        }

        start();
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public LocalStatementClient newLocalStatementClient(
        ExecutionContext executionContext, RelNode node) {
        if (statementResource == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, "Server not ready, is starting up");
        }
        return new LocalStatementClient(
            executionContext, node, statementResource, URI.create("http://" + serverHost + ":" + mppPort));
    }

    @Override
    public boolean isCoordinator() {
        return isMppServer;
    }
}
