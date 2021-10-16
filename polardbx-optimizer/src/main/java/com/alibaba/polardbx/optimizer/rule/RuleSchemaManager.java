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

package com.alibaba.polardbx.optimizer.rule;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.RepoSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 基于Rule获取到物理的group进行查找
 *
 * @since 5.0.0
 */
public class RuleSchemaManager extends AbstractLifecycle implements SchemaManager {

    private TddlRuleManager rule;
    private final Matrix matrix;
    private boolean useCache = true;
    private LoadingCache<Group, RepoSchemaManager> repos = null;
    private LoadingCache<String, TableMeta> cache = null;
    private RepoSchemaManager tableMetaManager = null;

    /**
     * when concurrent edit, only one get table meta data and edit
     */
    private final ConcurrentMap<String, Boolean> VALVES = Maps.newConcurrentMap();

    /**
     * default cache expire time, 30000ms
     */
    private long cacheExpireTime = TddlConstants.DEFAULT_TABLE_META_EXPIRE_TIME;

    public RuleSchemaManager(TddlRuleManager rule, RepoSchemaManager tableMetaManager, Matrix matrix,
                             Long cacheExpireTime) {
        this.rule = rule;
        this.matrix = matrix;

        this.tableMetaManager = tableMetaManager;

        if (cacheExpireTime != null && cacheExpireTime != 0) {
            this.cacheExpireTime = cacheExpireTime;
        }
    }

    @Override
    protected void doInit() {
        super.doInit();

        repos = CacheBuilder.newBuilder().build(new CacheLoader<Group, RepoSchemaManager>() {

            @Override
            public RepoSchemaManager load(Group group) throws Exception {
                RepoSchemaManager repo = new RepoSchemaManager(getSchemaName());
                repo.setGroup(group);
                repo.setRule(rule);
                repo.init();
                return repo;
            }
        });

        cache = CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheExpireTime, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<String, TableMeta>() {
                @Override
                public TableMeta load(String tableName) throws Exception {
                    return getTable0(tableName);
                }
            });

    }

    @Override
    protected void doDestroy() {
        super.doDestroy();

        for (RepoSchemaManager repo : repos.asMap().values()) {
            repo.destroy();
        }

        if (cache != null) {
            cache.cleanUp();
        }
    }

    private TableMeta getTable0(String tableName) {
        if (InformationSchema.NAME.equalsIgnoreCase(this.matrix.getSchemaName())) {
            return MetaDbSchema.getInstance().getTable(tableName);
        } else {
            return getTableMetaFromMetaDb(tableName);
        }
    }

    private TableMeta getTableMetaFromMetaDb(String tableName) {
        TableMeta ts = tableMetaManager.getTable(tableName);
        return ts;
    }

    @Override
    public TableMeta getTable(String tableName) {
        if (null != tableName) {
            tableName = tableName.toLowerCase();
        }

        TableMeta meta;
        if (useCache) {
            try {
                meta = cache.get(tableName);
            } catch (Throwable e) {
                cache.invalidate(tableName);// 允许再重试
                throw GeneralUtil.nestedException(e.getCause());
            }
        } else {
            meta = this.getTable0(tableName);
        }

        return meta;
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        if (useCache) {
            if (null != tableName) {
                tableName = tableName.toLowerCase();
            }
            cache.put(tableName, tableMeta);
        }
    }

    public Collection<TableMeta> getAllTables() {
        List<TableMeta> metas = new ArrayList<>();
        if (cache != null) {
            metas.addAll(cache.asMap().values());
        }
        return metas;
    }

    @Override
    public void invalidateAll() {
        this.cache.invalidateAll();
    }

    @Override
    public void invalidate(String tableName) {
        if (null != tableName) {
            tableName = tableName.toLowerCase();
        }
        this.cache.invalidate(tableName);
    }

    @Override
    public void reload(String tableName) {
        if (null != tableName) {
            tableName = tableName.toLowerCase();
        }
        this.cache.refresh(tableName);
    }

    public void setRule(TddlRuleManager rule) {
        this.rule = rule;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public TableMeta getTable(String groupName, String tableName, String actralTableName) {
        TableMeta ts = null;
        if (VALVES.putIfAbsent(tableName, true) != null) {
            return ts;
        }
        try {
            Group group = matrix.getGroup(groupName);
            ts = repos.get(group).getTable(tableName, actralTableName);
            cache.asMap().put(tableName, ts);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            VALVES.remove(tableName);
        }
        return ts;
    }

    @Override
    public GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        final Group defaultGroup = matrix.getGroup(rule.getDefaultDbIndex(null));
        try {
            return repos.get(defaultGroup).getGsi(primaryOrIndexTableName, statusSet);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public TableMeta getTableMetaFromConnection(String tableName, Connection conn) {
        final Group defaultGroup = matrix.getGroup(rule.getDefaultDbIndex(null));
        try {
            return repos.get(defaultGroup).getTableMetaFromConnection(tableName, conn);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public String getSchemaName() {
        return this.matrix.getSchemaName();
    }

    @Override
    public Map<String, TableMeta> getCache() {
        return cache.asMap();
    }

    @Override
    public TddlRuleManager getTddlRuleManager() {
        return rule;
    }
}
