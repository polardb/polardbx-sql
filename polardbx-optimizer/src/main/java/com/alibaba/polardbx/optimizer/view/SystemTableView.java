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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.alibaba.polardbx.common.utils.TStringUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dylan
 */
public interface SystemTableView {

    /**
     * check system table exists
     */
    Cache<String, Boolean> APPNAME_VIEW_ENABLED = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build();

    static void invalidateAll() {
        APPNAME_VIEW_ENABLED.invalidateAll();
    }

    static void invalidateCache(String schemaName) {
        if (TStringUtil.isBlank(schemaName)) {
            return;
        }
        schemaName = schemaName.toLowerCase();
        APPNAME_VIEW_ENABLED.invalidate(schemaName);
    }

    String getTableName();

    void resetDataSource(DataSource dataSource);

    void createTableIfNotExist();

    Row select(String viewName);

    boolean delete(String viewName);

    boolean deleteAll(Connection conn);

    boolean recordPlanError(String schemaName, String viewName, String planError);

    boolean insert(String viewName, List<String> columnList, String viewDefinition, String definer, String planString
        , String planType);

    boolean replace(String viewName, List<String> columnList, String viewDefinition, String definer,
                    String planString, String planType);

    int count(String schemaName);

    class Row {

        private String schemaName;

        private String viewName;

        private List<String> columnList;

        private String viewDefinition;

        private Boolean isVirtual = null;

        private String plan;

        private String planType;

        public Row(String schemaName, String viewName, List<String> columnList, String viewDefinition) {
            this(schemaName, viewName, columnList, viewDefinition, null, null);
        }

        public Row(String schemaName, String viewName, List<String> columnList, String viewDefinition, String plan,
                   String planType) {
            this.schemaName = schemaName;
            this.viewName = viewName;
            this.columnList = columnList;
            this.viewDefinition = viewDefinition;
            this.plan = plan;
            this.planType = planType;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getViewName() {
            return viewName;
        }

        public List<String> getColumnList() {
            return columnList;
        }

        public String getViewDefinition() {
            return viewDefinition;
        }

        public boolean isVirtual() {
            if (isVirtual != null) {
                return isVirtual;
            }
            isVirtual = isVirtual(viewDefinition);
            return isVirtual;
        }

        public static boolean isVirtual(String s) {
            try {
                getVirtualViewType(s.toUpperCase());
                return true;
            } catch (Throwable t) {
                return false;
            }
        }

        public static VirtualViewType getVirtualViewType(String s) {
            return VirtualViewType.valueOf(s.toUpperCase());
        }

        public VirtualViewType getVirtualViewType() {
            return getVirtualViewType(viewName);
        }

        public String getPlan() {
            return plan;
        }

        public String getPlanType() {
            return planType;
        }

        public boolean isMppPlanType() {
            return planType != null && planType.equalsIgnoreCase("MPP");
        }

        public boolean isSmpPlanType() {
            return planType != null && planType.equalsIgnoreCase("SMP");
        }
    }
}
