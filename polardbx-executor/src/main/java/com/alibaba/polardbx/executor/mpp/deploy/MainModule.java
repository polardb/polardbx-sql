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
import com.google.inject.TypeLiteral;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.discover.ClusterNodeManager;
import com.alibaba.polardbx.executor.mpp.execution.LocationFactory;
import com.alibaba.polardbx.executor.mpp.execution.StageInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.metadata.ForNodeManager;
import com.alibaba.polardbx.executor.mpp.metadata.HandleJsonModule;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientFactory;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.executor.mpp.operator.ForExchange;
import com.alibaba.polardbx.executor.mpp.operator.ForScheduler;
import com.alibaba.polardbx.executor.mpp.server.ForAsyncHttp;
import com.alibaba.polardbx.executor.mpp.server.PagesResponseWriter;
import com.alibaba.polardbx.executor.mpp.server.TaskUpdateRequest;
import com.alibaba.polardbx.executor.mpp.server.remotetask.HttpLocationFactory;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.executor.mpp.web.FailureDetectorModule;
import com.alibaba.polardbx.executor.mpp.web.NodeResource;
import com.alibaba.polardbx.executor.mpp.web.ServerInfoResource;
import com.alibaba.polardbx.executor.mpp.web.WebUiResource;
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
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Singleton;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.executor.mpp.client.MppMediaTypes.MPP_POLARDBX;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MainModule extends BaseModule {

    private InternalNode currentNode;

    public MainModule(InternalNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public void setup(Binder binder) {
        binder.bind(InternalNode.class).toInstance(currentNode);
        binder.bind(InternalNodeManager.class).to(ClusterNodeManager.class).in(Scopes.SINGLETON);

        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);
        // handle resolver
        binder.install(new HandleJsonModule());

        // splits
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);

        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(InternalNode.class);
        jsonCodecBinder(binder).bindJsonCodec(AllNodes.class);
        jaxrsBinder(binder).bind(PagesResponseWriter.class);

        // task executor
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();

        // exchange client
        binder.bind(new TypeLiteral<ExchangeClientSupplier>() {
        }).to(ExchangeClientFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("exchange", ForExchange.class)
//				.withTracing()
            .withConfigDefaults(config -> {
                config.setIdleTimeout(new Duration(10, TimeUnit.HOURS));
                config.setRequestTimeout(new Duration(60 * 5, SECONDS));
                config.setConnectTimeout(new Duration(60 * 5, SECONDS));
                config.setMaxContentLength(new DataSize(512, MEGABYTE));
                config.setMaxThreads(MppConfig.getInstance().getHttpClientMaxThreads());
                config.setMinThreads(MppConfig.getInstance().getHttpClientMinThreads());
                config.setMaxRequestsQueuedPerDestination(MppConfig.getInstance().getHttpMaxRequestsPerDestination());
                config.setMaxConnectionsPerServer(
                    MppConfig.getInstance().getDefaultMppHttpClientMaxConnectionsPerServer());
                config.setMaxConnections(MppConfig.getInstance().getHttpClientMaxConnections());
            });

        // http client
        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
            .withConfigDefaults(config -> {
                config.setIdleTimeout(new Duration(10, TimeUnit.HOURS));
                config.setRequestTimeout(new Duration(60 * 60, SECONDS));
                config.setConnectTimeout(new Duration(60 * 60, SECONDS));
                config.setMaxThreads(MppConfig.getInstance().getHttpClientMaxThreads());
                config.setMinThreads(MppConfig.getInstance().getHttpClientMinThreads());
                config.setMaxRequestsQueuedPerDestination(MppConfig.getInstance().getHttpMaxRequestsPerDestination());
                config.setMaxConnectionsPerServer(
                    MppConfig.getInstance().getDefaultMppHttpClientMaxConnectionsPerServer());
                config.setMaxConnections(MppConfig.getInstance().getHttpClientMaxConnections());
            });

        httpClientBinder(binder).bindHttpClient("node-manager", ForNodeManager.class)
            .withConfigDefaults(config -> {
                config.setIdleTimeout(new Duration(30, SECONDS));
                config.setRequestTimeout(new Duration(10, SECONDS));
            });

        //---------------- web ui ----------------------
        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("index.html");
        httpServerBinder(binder).bindResource("/tableau", "webapp/tableau");
        jaxrsBinder(binder).bind(WebUiResource.class);

        // Determine the NodeVersion
        NodeVersion nodeVersion = new NodeVersion(Version.getVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);
        jaxrsBinder(binder).bind(ServerInfoResource.class);
        discoveryBinder(binder).bindSelector(MPP_POLARDBX);
        binder.install(new FailureDetectorModule());
        jaxrsBinder(binder).bind(NodeResource.class);
        //---------------- web ui ----------------------

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

    @Provides
    @Singleton
    @ForExchange
    public static ScheduledExecutorService createExchangeExecutor() {
        return newScheduledThreadPool(MppConfig.getInstance().getExchangeClientThreads(),
            Threads.daemonThreadsNamed("exchange-client"));
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static ScheduledExecutorService createAsyncHttpTimeoutExecutor() {
        return newScheduledThreadPool(MppConfig.getInstance().getHttpTimeoutThreads(),
            Threads.daemonThreadsNamed("async-http-timeout"));
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static BoundedExecutor createAsyncHttpResponseExecutor() {
        int poolSize = MppConfig.getInstance().getHttpResponseThreads();
        ExecutorService coreExecutor = newFixedThreadPool(poolSize,
            Threads.daemonThreadsNamed("async-http-response"));
        return new BoundedExecutor(coreExecutor, MppConfig.getInstance().getHttpResponseThreads());
    }
}
