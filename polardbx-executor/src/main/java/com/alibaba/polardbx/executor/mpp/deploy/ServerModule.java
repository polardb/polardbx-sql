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

import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTaskFactory;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryLocalExecution;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryManager;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.NodeScheduler;
import com.alibaba.polardbx.executor.mpp.planner.NodePartitioningManager;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.executor.mpp.server.remotetask.HttpRemoteTaskFactory;
import com.alibaba.polardbx.executor.mpp.util.FinalizerService;
import com.alibaba.polardbx.executor.mpp.web.ClusterStatsResource;
import com.alibaba.polardbx.executor.mpp.web.DdlResource;
import com.alibaba.polardbx.executor.mpp.web.ForQueryInfo;
import com.alibaba.polardbx.executor.mpp.web.QueryResource;
import com.alibaba.polardbx.executor.mpp.web.StageResource;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ServerModule extends AbstractConfigurationAwareModule {

    @Override
    public void setup(Binder binder) {
        // execution scheduler
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();

        // Finalizer
        binder.bind(FinalizerService.class).in(Scopes.SINGLETON);
        //queryExecutionFactory
        binder.bind(NodeTaskMap.class).in(Scopes.SINGLETON);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
        binder.bind(NodePartitioningManager.class).in(Scopes.SINGLETON);
        binder.bind(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);

        //localExecutionFactory
        binder.bind(SqlQueryLocalExecution.SqlQueryLocalExecutionFactory.class).in(Scopes.SINGLETON);

        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(StatementResource.class);
        newExporter(binder).export(StatementResource.class).withGeneratedName();

        //---------------- server web ui ----------------------
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(DdlResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(ClusterStatsResource.class);
        httpClientBinder(binder).bindHttpClient("queryInfo", ForQueryInfo.class);
        //---------------- server web ui ----------------------
    }
}
