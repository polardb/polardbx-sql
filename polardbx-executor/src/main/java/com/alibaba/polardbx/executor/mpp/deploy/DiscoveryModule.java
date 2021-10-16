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

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.alibaba.polardbx.executor.mpp.Threads;
import io.airlift.configuration.ConfigBinder;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.CachingServiceSelectorFactory;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.MergingServiceSelectorFactory;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.ServiceState;
import io.airlift.discovery.client.testing.InMemoryDiscoveryClient;
import io.airlift.http.client.HttpClientBinder;
import io.airlift.json.JsonCodecBinder;
import io.airlift.node.NodeInfo;
import org.weakref.jmx.guice.ExportBinder;

import javax.annotation.PreDestroy;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DiscoveryModule implements Module {
    public DiscoveryModule() {
    }

    public void configure(Binder binder) {
        binder.bind(ServiceInventory.class).in(Scopes.SINGLETON);
        ConfigBinder.configBinder(binder).bindConfig(ServiceInventoryConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(DiscoveryClientConfig.class);
        binder.bind(DiscoveryLookupClient.class).to(InMemoryDiscoveryClient.class).in(Scopes.SINGLETON);
        binder.bind(DiscoveryAnnouncementClient.class).to(InMemoryDiscoveryClient.class).in(Scopes.SINGLETON);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(ServiceDescriptorsRepresentation.class);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(Announcement.class);
        HttpClientBinder.httpClientBinder(binder).bindHttpClient("discovery", ForDiscoveryClient.class);
        binder.bind(Announcer.class).in(Scopes.SINGLETON);
        ExportBinder.newExporter(binder).export(Announcer.class).withGeneratedName();
        Multibinder.newSetBinder(binder, ServiceAnnouncement.class);
        binder.bind(CachingServiceSelectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(ServiceSelectorFactory.class).to(MergingServiceSelectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(ScheduledExecutorService.class).annotatedWith(ForDiscoveryClient.class)
            .toProvider(DiscoveryModule.DiscoveryExecutorProvider.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder, ServiceSelector.class);
        binder.bind(ServiceSelectorManager.class).in(Scopes.SINGLETON);
        ExportBinder.newExporter(binder).export(ServiceInventory.class).withGeneratedName();
    }

    @Provides
    @ForDiscoveryClient
    public URI getDiscoveryUri(ServiceInventory serviceInventory, DiscoveryClientConfig config) {
        Iterable<ServiceDescriptor> discovery = serviceInventory.getServiceDescriptors("discovery");
        Iterator var4 = discovery.iterator();

        while (true) {
            ServiceDescriptor descriptor;
            do {
                if (!var4.hasNext()) {
                    if (config != null) {
                        return config.getDiscoveryServiceURI();
                    }

                    return null;
                }

                descriptor = (ServiceDescriptor) var4.next();
            } while (descriptor.getState() != ServiceState.RUNNING);

            try {
                return new URI((String) descriptor.getProperties().get("https"));
            } catch (Exception var8) {
                try {
                    return new URI((String) descriptor.getProperties().get("http"));
                } catch (Exception var7) {
                    ;
                }
            }
        }
    }

    @Provides
    @Singleton
    public MergingServiceSelectorFactory createMergingServiceSelectorFactory(CachingServiceSelectorFactory factory,
                                                                             Announcer announcer, NodeInfo nodeInfo) {
        return new MergingServiceSelectorFactory(factory, announcer, nodeInfo);
    }

    private static class DiscoveryExecutorProvider implements Provider<ScheduledExecutorService> {
        private ScheduledExecutorService executor;

        private DiscoveryExecutorProvider() {
        }

        @Override
        public ScheduledExecutorService get() {
            Preconditions.checkState(this.executor == null, "provider already used");
            this.executor = new ScheduledThreadPoolExecutor(5, Threads.daemonThreadsNamed("Discovery"));
            return this.executor;
        }

        @PreDestroy
        public void destroy() {
            if (this.executor != null) {
                this.executor.shutdownNow();
            }
        }
    }
}
