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

package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.TddlGroupExecutor;
import com.alibaba.polardbx.executor.repo.RepositoryConfig;
import com.alibaba.polardbx.executor.spi.ICommandHandlerFactory;
import com.alibaba.polardbx.executor.spi.ICursorFactory;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.handler.CommandHandlerFactoryMyImp;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class MyRepository extends AbstractLifecycle implements IRepository {

    private transient LoadingCache<Group, IGroupExecutor> executors;

    private RepositoryConfig config;

    private MyDataSourceGetter dsGetter = null;
    private String appName = "";

    private String schemaName = "";

    private CursorFactoryMyImpl cfm;
    private ICommandHandlerFactory cef = null;

    public MyRepository() {
    }

    @Override
    public void doInit() {
        if (schemaName == null) {
            schemaName = appName;
        }
        this.dsGetter = new MyDataSourceGetter(schemaName);
        this.config = new RepositoryConfig();
        this.config.setProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION, "READ_COMMITTED");
        this.config.setProperty(RepositoryConfig.IS_TRANSACTIONAL, "true");
        cfm = new CursorFactoryMyImpl(this);
        cef = new CommandHandlerFactoryMyImp(this);

        executors = CacheBuilder.newBuilder().build(new CacheLoader<Group, IGroupExecutor>() {

            @Override
            public TddlGroupExecutor load(Group group) {
                TGroupDataSource groupDS =
                    new TGroupDataSource(group.getName(), group.getSchemaName(), group.getAppName(),
                        group.getUnitName());
                groupDS.init();

                TddlGroupExecutor executor = new TddlGroupExecutor(getRepo());
                executor.setGroup(group);
                executor.setGroupDataSource(groupDS);
                executor.init();

                return executor;
            }
        });
    }

    protected IRepository getRepo() {
        return this;
    }

    @Override
    protected void doDestroy() {
        for (IGroupExecutor executor : executors.asMap().values()) {
            executor.destroy();
        }
    }

    @Override
    public RepositoryConfig getRepoConfig() {
        return config;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        return cfm;
    }

    @Override
    public ICommandHandlerFactory getCommandExecutorFactory() {
        return cef;
    }

    @Override
    public IGroupExecutor getGroupExecutor(final Group group) {
        try {

            return executors.get(group);
        } catch (Throwable e) {
            executors.invalidate(group);
            throw GeneralUtil.nestedException(e.getCause());
        }
    }

    @Override
    public void invalidateGroupExecutor(Group group) {
        executors.invalidate(group);
    }

    public TGroupDataSource getDataSource(String groupName) {
        return dsGetter.getDataSource(schemaName, groupName);
    }

    public TGroupDataSource getDataSource(String schemaName, String groupName) {
        return dsGetter.getDataSource(schemaName, groupName);
    }

    protected MyJdbcHandler createQueryHandler(ExecutionContext executionContext) {
        return new MyJdbcHandler(executionContext, this);
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
