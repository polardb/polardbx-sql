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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.discover.LocalNodeManager;
import com.alibaba.polardbx.executor.mpp.execution.LocationFactory;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryLocalExecution;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryManager;
import com.alibaba.polardbx.executor.mpp.execution.StageInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.metadata.ForNodeManager;
import com.alibaba.polardbx.executor.mpp.metadata.HandleJsonModule;
import com.alibaba.polardbx.executor.mpp.server.PagesResponseWriter;
import com.alibaba.polardbx.executor.mpp.server.remotetask.HttpLocationFactory;
import com.alibaba.polardbx.executor.mpp.util.FinalizerService;
import com.alibaba.polardbx.executor.mpp.web.ClusterStatsResource;
import com.alibaba.polardbx.executor.mpp.web.DdlResource;
import com.alibaba.polardbx.executor.mpp.web.FailureDetectorModule;
import com.alibaba.polardbx.executor.mpp.web.ForQueryInfo;
import com.alibaba.polardbx.executor.mpp.web.ForWorkerInfo;
import com.alibaba.polardbx.executor.mpp.web.NodeResource;
import com.alibaba.polardbx.executor.mpp.web.QueryResource;
import com.alibaba.polardbx.executor.mpp.web.ServerInfoResource;
import com.alibaba.polardbx.executor.mpp.web.StageResource;
import com.alibaba.polardbx.executor.mpp.web.StatusResource;
import com.alibaba.polardbx.executor.mpp.web.ThreadResource;
import com.alibaba.polardbx.executor.mpp.web.WebUiResource;
import com.alibaba.polardbx.executor.mpp.web.WorkerResource;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileCleaner;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.FileCleaner;
import com.alibaba.polardbx.executor.operator.spill.ForAsyncSpill;
import com.alibaba.polardbx.executor.operator.spill.GenericSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.NodeVersion;
import io.airlift.units.Duration;

import javax.inject.Singleton;
import java.util.concurrent.ExecutorService;

import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_POLARDBX;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class LocalModule extends BaseModule {

    private InternalNode currentNode;

    public LocalModule(InternalNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public void setup(Binder binder) {

        // Finalizer
        binder.bind(FinalizerService.class).in(Scopes.SINGLETON);

        binder.bind(InternalNode.class).toInstance(currentNode);
        binder.bind(InternalNodeManager.class).to(LocalNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);

        //queryExecutionFactory
        binder.bind(SqlQueryExecution.SqlQueryExecutionFactory.class).toInstance(
            new SqlQueryExecution.NullExecutionFactory());

        //localExecutionFactory
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();
        binder.bind(SqlQueryLocalExecution.SqlQueryLocalExecutionFactory.class).in(Scopes.SINGLETON);

        //QueryManager
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());

        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(InternalNode.class);
        jsonCodecBinder(binder).bindJsonCodec(AllNodes.class);
        jaxrsBinder(binder).bind(PagesResponseWriter.class);

        httpClientBinder(binder).bindHttpClient("node-manager", ForNodeManager.class)
            .withConfigDefaults(config -> {
                config.setIdleTimeout(new Duration(30, SECONDS));
                config.setRequestTimeout(new Duration(10, SECONDS));
            });

        // Determine the NodeVersion
        NodeVersion nodeVersion = new NodeVersion(Version.getVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);
        jaxrsBinder(binder).bind(ServerInfoResource.class);
        discoveryBinder(binder).bindSelector(MPP_POLARDBX);
        binder.install(new FailureDetectorModule());
        jaxrsBinder(binder).bind(NodeResource.class);

        //---------------- web ui ----------------------
        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("index.html");
        httpServerBinder(binder).bindResource("/tableau", "webapp/tableau");
        jaxrsBinder(binder).bind(WebUiResource.class);

        //---------------- server web ui ----------------------
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(ClusterStatsResource.class);
        jaxrsBinder(binder).bind(DdlResource.class);
        httpClientBinder(binder).bindHttpClient("queryInfo", ForQueryInfo.class);

        //---------------- worker web ui ----------------------
        jaxrsBinder(binder).bind(ThreadResource.class);
        jaxrsBinder(binder).bind(WorkerResource.class);
        jaxrsBinder(binder).bind(StatusResource.class);
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);
        //----------------worker web ui ----------------------

        // Spiller
        binder.bind(FileCleaner.class).to(AsyncFileCleaner.class).in(Scopes.SINGLETON);
        binder.bind(SpillerFactory.class).to(GenericSpillerFactory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForAsyncSpill
    public static ExecutorService createAsyncSpillCleanerExecutor() {
        return newFixedThreadPool(1, Threads.daemonThreadsNamed("async-spill"));
    }

    @Provides
    @Singleton
    public static SingleStreamSpillerFactory createSingleStreamSpillerFactory(FileCleaner fileCleaner) {
        return new AsyncFileSingleStreamSpillerFactory(fileCleaner).cleanupOldSpillFiles();
    }
}
