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

package com.alibaba.polardbx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.QuarantineConfig;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.config.UserConfig;
import com.alibaba.polardbx.config.loader.ClusterLoader;
import com.alibaba.polardbx.config.loader.GmsClusterLoader;
import com.alibaba.polardbx.config.loader.ServerLoader;
import com.alibaba.polardbx.server.util.MockUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class CobarConfig extends AbstractLifecycle implements Lifecycle {
    protected static final Logger logger = LoggerFactory.getLogger(CobarConfig.class);
    private volatile ServerLoader serverLoader;
    private volatile ClusterLoader clusterLoader;
    private volatile String cluster;
    private volatile String unitName;
    private volatile String instanceId;
    private volatile QuarantineConfig quarantine;
    private volatile AuthorizeConfig authorizeConfig;

    public CobarConfig() {
        initCobarConfig();
    }

    protected void initCobarConfig() {

        try {
            serverLoader = new ServerLoader();
            serverLoader.init();

            quarantine = new QuarantineConfig();
            List<String> trustIpList = new ArrayList<String>();
            String trustIps = serverLoader.getSystem().getTrustedIps();
            if (StringUtils.isNotEmpty(trustIps)) {
                trustIpList.add(trustIps);
            }

            trustIps = StringUtils.join(trustIpList, ',');
            quarantine.resetTrustedIps(trustIps);
            if (!ConfigDataMode.isFastMock()) {
                initTrupIpListener();
            }
            quarantine.resetBlackList(serverLoader.getSystem().getBlackIps());
            quarantine.resetWhiteList(serverLoader.getSystem().getWhiteIps());

            // 初始化授权相关配置
            authorizeConfig = new AuthorizeConfig();
        } catch (Throwable ex) {
            logger.error("Failed to init cobar server.", ex);
            throw ex;
        }
    }

    /**
     * 动态修改trustIps
     */
    private void initTrupIpListener() {
        changeTrustIps("");
    }

    private void changeTrustIps(String data) {
        if (StringUtils.isEmpty(data)) {
            return;
        }
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = new ByteArrayInputStream(data.getBytes());
            properties.load(in);
            List<String> trustIpList = Lists.newArrayList();
            String trustIps = properties.getProperty("trustIps");
            if (StringUtils.isNotEmpty(trustIps)) {
                String oldTrustedIps = serverLoader.getSystem().getTrustedIps();
                if (StringUtils.isNotEmpty(oldTrustedIps)) {
                    List<String> array = Lists.newArrayList();
                    array.add(oldTrustedIps);
                    array.add(trustIps);
                    trustIps = StringUtils.join(array, ',');
                }
                serverLoader.getSystem().setTrustedIps(trustIps);
                trustIpList.add(trustIps);
            } else {
                return;
            }

            trustIps = StringUtils.join(trustIpList, ',');
            quarantine.resetTrustedIps(trustIps);

        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    protected void doInit() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        ConfigDataMode.setMode(ConfigDataMode.Mode.MANAGER);
        this.cluster = serverLoader.getSystem().getClusterName();
        this.unitName = serverLoader.getSystem().getUnitName();
        this.instanceId = serverLoader.getSystem().getInstanceId();

        //clusterLoader = new ManagerClusterLoader(cluster, unitName);
        clusterLoader = new GmsClusterLoader(serverLoader.getSystem());
        clusterLoader.init();
        authorizeConfig.init();
    }

    @Override
    protected void doDestroy() {
        clusterLoader.destroy();
        serverLoader.destroy();
    }

    public QuarantineConfig getClusterQuarantine() {
        return quarantine;
    }

    public SystemConfig getSystem() {
        return serverLoader.getSystem();
    }

    public Map<String/* user */, UserConfig> getUsers() {
        if (clusterLoader != null) {
            return clusterLoader.getAppLoader().getUsers();
        } else {
            return null;
        }
    }

    public AuthorizeConfig getAuthorizeConfig() {
        return authorizeConfig;
    }

    public Map<String, SchemaConfig> getSchemas() {
        Map<String, SchemaConfig> caseInsensitiveSchemaConfigs = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, SchemaConfig> orignalSchemaConfigs = null;
        if (clusterLoader != null && clusterLoader.getAppLoader() != null) {
            orignalSchemaConfigs = clusterLoader.getAppLoader().getSchemas();
        } else {
            if (ConfigDataMode.isFastMock()) {
                orignalSchemaConfigs = MockUtil.schemas();
            } else {
                // 太多地方使用,避免返回null值出现NPE
                orignalSchemaConfigs = Maps.newConcurrentMap();
            }
        }
        caseInsensitiveSchemaConfigs.putAll(orignalSchemaConfigs);
        return caseInsensitiveSchemaConfigs;
    }

    public QuarantineConfig getQuarantine() {
        if (clusterLoader != null) {
            return clusterLoader.getAppLoader().getQuarantine();
        } else {
            return null;
        }
    }

    public boolean isLock() {
        if (clusterLoader != null) {
            return clusterLoader.isLock();
        } else {
            return false;
        }
    }

    public void reloadCluster(String cluster, String unitName, String instanceId) {

        if (!StringUtils.equals(instanceId, this.instanceId)) {
            // 出现instanceId变动
            if (clusterLoader != null) {
                if (CobarServer.getInstance().isOnline()) {
                    serverLoader.getSystem().setInstanceId(this.instanceId);
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG,
                        "change cluster should offline server, so ignore. current instance = " + instanceId);
                }
                clusterLoader.destroy();
            }

            if (StringUtils.isEmpty(instanceId)) {
                return;
            }
            this.cluster = null; // 清空
            this.unitName = null;
            this.serverLoader.getSystem().setClusterName(null);
            this.serverLoader.getSystem().setUnitName(null);
            this.clusterLoader = new GmsClusterLoader(serverLoader.getSystem());
            this.instanceId = instanceId;
            this.clusterLoader.init();
        }
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getInstanceId() {
        return this.instanceId;
    }

    public ClusterLoader getClusterLoader() {
        return clusterLoader;
    }

    public String getCluster() {
        return cluster;
    }

}
