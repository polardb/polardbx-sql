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

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.config.ConfigDataMode;
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
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CobarConfig extends AbstractLifecycle implements Lifecycle {
    protected static final Logger logger = LoggerFactory.getLogger(CobarConfig.class);
    private volatile ServerLoader serverLoader;
    private volatile ClusterLoader clusterLoader;
    private volatile String cluster;
    private volatile String instanceId;
    private volatile QuarantineConfig quarantine;
    private volatile AuthorizeConfig authorizeConfig;

    public CobarConfig() {
        enableFastJsonAutoType();
        initCobarConfig();
    }

    private void enableFastJsonAutoType() {
        try {
            ParserConfig.getGlobalInstance();
            ParserConfig.getGlobalInstance().addAccept("com.taobao.tddl.");
            ParserConfig.getGlobalInstance().addAccept("com.alibaba.tddl.");
            ParserConfig.getGlobalInstance().addAccept("com.alibaba.polardbx.");
            ParserConfig.getGlobalInstance().addAccept("org.apache.calcite.");
        } catch (Throwable e) {
            // ignore
        }
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
     * <pre>
     *
     *  CobarConfig 的第一个阶段初始化的动作 （即initCobarConfig）：
     *
     *  1. 将
     *          server.propeties
     *      /   JVM 启动参数
     *      /   com.taobao.tddl.monitor
     *      /   com.taobao.tddl.trustips （信任IP与白名单）
     *      的所有属性全部读取，并放到System.properties中,
     *      并同时将部分System.properties保存到ServerLoader.SystemConfig中
     *
     *  2.  订阅了  com.taobao.tddl.monitor 与  com.taobao.tddl.trustips 这两个dataId的Diamond变更，
     *      并将这个变更信息实时保存在ServerLoader.SystemConfig（但没有变更System.properties中的值）
     *
     *  3. 更新ConfigDataMode的信息
     *
     *  //=======================
     *
     *  CobarConfig 的第二个阶段初始化的动作 （即doInit）：
     *
     *   1.
     *   如果是cluster模式，加载cluster集群配置,
     *   如果是drds模式，从 InstanceInfoEvent 中加载实例配置，
     *   并保存到ServerLoader.SystemConfig中（但没有变更System.properties中的值）
     *
     *   2. 从 com.taobao.tddl.instance_properties.xxx_cluster
     *      中获取实例所对应的集群的Diamond的动态配置，并订阅其变更，
     *      将最新的配置信息保存到ServerLoader.SystemConfig中（但没有变更System.properties中的值）
     * </pre>
     */
    @Override
    protected void doInit() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        this.cluster = serverLoader.getSystem().getClusterName();
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
