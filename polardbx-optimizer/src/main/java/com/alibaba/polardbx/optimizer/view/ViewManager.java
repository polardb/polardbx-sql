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

package com.alibaba.polardbx.optimizer.view;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class ViewManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ViewManager.class);

    private final String schemaName;

    private SystemTableView systemTableView;

    private final ParamManager paramManager;

    private LoadingCache<String, SystemTableView.Row> cache = null;

    // viewName -> (column, viewDefinition)
    public static Map<String, Pair<List<String>, String>> predefineViews;

    static {
        predefineViews = TreeMaps.caseInsensitiveMap();

        defineVirtualView(VirtualViewType.VIRTUAL_STATISTIC, new String[] {
            "SCHEMA_NAME",
            "TABLE_NAME",
            "TABLE_ROWS",
            "COLUMN_NAME",
            "CARDINALITY",
            "NDV_SOURCE",
            "TOPN",
            "HISTOGRAM",
            "LAST_MODIFY_TIME",
            "LAST_ACCESS_TIME"
        });
    }

    private static void defineView(String name, String[] columns, String definition) {
        predefineViews.put(name,
            Pair.of(columns == null ? null : Arrays.stream(columns).collect(Collectors.toList()), definition));
    }

    private static void defineVirtualView(VirtualViewType virtualViewType, String[] columns) {
        defineView(virtualViewType.name(), columns, virtualViewType.name());
    }

    public ViewManager(String schemaName,
                       SystemTableView systemTableView,
                       Map<String, Object> connectionProperties) {
        this.schemaName = schemaName;
        this.systemTableView = systemTableView;
        this.paramManager = new ParamManager(connectionProperties);
    }

    @Override
    protected void doInit() {
        super.doInit();

        if (ConfigDataMode.isMasterMode()) {
            systemTableView.createTableIfNotExist();
        }

        ViewManager that = this;

        cache = CacheBuilder.newBuilder()
            .refreshAfterWrite(TddlConstants.DEFAULT_VIEW_CACHE_EXPIRE_TIME, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<String, SystemTableView.Row>() {

                @Override
                public SystemTableView.Row load(String viewName) throws Exception {
                    return that.innerSelect(viewName);
                }
            });
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();

        if (cache != null) {
            cache.cleanUp();
        }
    }

    public SystemTableView getSystemTableView() {
        return systemTableView;
    }

    public void invalidate(String viewName) {
        viewName = viewName.toLowerCase();
        this.cache.invalidate(viewName);
    }

    public SystemTableView.Row select(String viewName) {
        viewName = viewName.toLowerCase();
        try {
            return cache.get(viewName);
        } catch (Throwable e) {
            cache.invalidate(viewName);
            logger.debug(e);
            return null;
        }
    }

    public List<String> selectAllViewName() {
        return systemTableView.selectAllViewName();
    }

    private SystemTableView.Row innerSelect(String viewName) {
        Pair<List<String>, String> pair = predefineViews.get(viewName);
        if (pair != null) {
            return new SystemTableView.Row(schemaName, viewName, pair.getKey(), pair.getValue());
        }
        return systemTableView.select(viewName);
    }

    public boolean insert(String viewName, List<String> columnList, String viewDefinition, String definer,
                          String planString, String planType) {
        if (predefineViews.containsKey(viewName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "can not create predefined view");
        }
        return systemTableView.insert(viewName, columnList, viewDefinition, definer, planString, planType);
    }

    public boolean replace(String viewName, List<String> columnList, String viewDefinition, String definer,
                           String planString, String planType) {
        if (predefineViews.containsKey(viewName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "can not create predefined view");
        }
        return systemTableView.replace(viewName, columnList, viewDefinition, definer, planString, planType);
    }

    public boolean delete(String viewName) {
        if (predefineViews.containsKey(viewName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "can not drop predefined view");
        }
        return systemTableView.delete(viewName);
    }

    public boolean recordPlanError(String schemaName, String viewName, String planError) {
        if (predefineViews.containsKey(viewName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "can not update predefined view");
        }
        return systemTableView.recordPlanError(schemaName, viewName, planError);
    }

    public int count(String schemaName) {
        return systemTableView.count(schemaName);
    }
}
