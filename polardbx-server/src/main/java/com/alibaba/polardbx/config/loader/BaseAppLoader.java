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

package com.alibaba.polardbx.config.loader;

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.QuarantineConfig;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.UserConfig;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public abstract class BaseAppLoader extends AbstractLifecycle implements Lifecycle {

    private static final Logger logger = LoggerFactory.getLogger(BaseAppLoader.class);
    protected String cluster;
    protected String unitName;
    protected QuarantineConfig quarantine;
    protected Set<String> loadedApps = new HashSet<String>();
    protected Map<String/* user */, UserConfig> users = Maps.newConcurrentMap();
    protected Map<String, SchemaConfig> schemas = Maps.newConcurrentMap();

    public BaseAppLoader(String cluster, String unitName) {
        this.cluster = cluster;
        this.unitName = unitName;
        this.quarantine = new QuarantineConfig();
    }

    @Override
    protected void doInit() {
    }

    @Override
    protected void doDestroy() {

        for (SchemaConfig schema : schemas.values()) {
            TDataSource dataSource = schema.getDataSource();
            try {
                if (dataSource != null) {
                    dataSource.destroy();
                }

                this.quarantine.cleanHostByApp(schema.getName());
            } catch (Exception e) {
                // ignore
            }
        }

        this.loadApps(new ArrayList<String>());
    }

    public void loadApps(List<String> apps) {
        // config: app1,app2,...
        logger.info("start loading apps in cluster:" + this.cluster);
        long startTime = TimeUtil.currentTimeMillis();

        // Try to load 'polardbx' app first. In this way, some global
        // timer tasks will be initialized by 'polardbx' schema,
        // and the log file will be written under tddl file folder.
        if (apps.contains(DEFAULT_DB_NAME)) {
            try {
                this.loadApp(DEFAULT_DB_NAME);
                this.loadedApps.add(DEFAULT_DB_NAME);
            } catch (Exception e) {
                logger.error("load app error:" + DEFAULT_DB_NAME, e);
            }
        }

        // 先添加不存在的app
        for (String app : apps) {
            if (!this.loadedApps.contains(app)) {
                try {
                    this.loadApp(app);
                    // 该APP的所有配置加载成功
                    this.loadedApps.add(app);
                } catch (Exception e) {
                    // 一个app加载失败,不能影响其他app的加载
                    logger.error("load app error:" + app, e);
                }
            }
        }

        List<String> appHistory = new ArrayList<String>(loadedApps);
        for (String app : appHistory) {
            if (!apps.contains(app)) {
                try {
                    this.unLoadApp(app);
                    this.loadedApps.remove(app);
                } catch (Exception e) {
                    // 一个app加载失败,不能影响其他app的加载
                    logger.error("unLoad app error:" + app, e);
                }
            }
        }

        long endTime = TimeUtil.currentTimeMillis();
        logger.info("finish loading apps in cluster with " + (endTime - startTime) + " ms");
    }

    /**
     * 装载app
     */
    public void loadApp(final String app) {
        try {
            logger.info("start loading app:" + app);
            this.loadSchema(app, app);
            this.loadMdlManager(app);

            // 该APP的所有配置加载成功
            this.loadedApps.add(app);
            logger.info("finish loading app:" + app);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * 卸载app
     */
    protected void unLoadApp(final String app) {
        try {
            logger.info("start unLoading app:" + app);
            this.unLoadSchema(app, app);
            this.loadMdlManager(app);
            logger.info("finish unLoading app:" + app);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    protected abstract void loadSchema(final String dbName, final String appName);

    protected abstract void unLoadSchema(final String dbName, final String appName);

    protected abstract void loadMdlManager(final String dbName);

    protected abstract void unloadMdlManager(final String dbName);

    // =======================================================

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public QuarantineConfig getQuarantine() {
        return quarantine;
    }
}
