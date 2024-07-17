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

import com.alibaba.polardbx.executor.mpp.execution.SqlTaskManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskManagementExecutor;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.server.TaskResource;
import com.alibaba.polardbx.executor.mpp.web.ForWorkerInfo;
import com.alibaba.polardbx.executor.mpp.web.StatusResource;
import com.alibaba.polardbx.executor.mpp.web.ThreadResource;
import com.alibaba.polardbx.executor.mpp.web.WorkerResource;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class WorkerModule extends AbstractConfigurationAwareModule {

    @Override
    public void setup(Binder binder) {
        // task execution
        jaxrsBinder(binder).bind(TaskResource.class);
        newExporter(binder).export(TaskResource.class).withGeneratedName();

        binder.bind(TaskManagementExecutor.class).in(Scopes.SINGLETON);
        binder.bind(SqlTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(Key.get(SqlTaskManager.class));

        newExporter(binder).export(TaskManager.class).withGeneratedName();

        //---------------- worker web ui ----------------------
        jaxrsBinder(binder).bind(ThreadResource.class);
        jaxrsBinder(binder).bind(WorkerResource.class);
        jaxrsBinder(binder).bind(StatusResource.class);
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);
        //----------------worker web ui ----------------------
    }
}
