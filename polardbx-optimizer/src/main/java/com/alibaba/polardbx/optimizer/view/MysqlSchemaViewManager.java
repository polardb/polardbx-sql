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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.schema.MysqlSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class MysqlSchemaViewManager extends ViewManager {

    // viewName -> (column, viewDefinition)
    private Map<String, Pair<List<String>, String>> mysqlSchemaViews;

    private static final MysqlSchemaViewManager INSTANCE;

    static {
        INSTANCE = new MysqlSchemaViewManager();
        INSTANCE.init();
    }

    private MysqlSchemaViewManager() {
        super(null, null, null);
    }

    public static MysqlSchemaViewManager getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        mysqlSchemaViews = TreeMaps.caseInsensitiveMap();
        definePolarXView();
    }

    private void definePolarXView() {
        defineView("user", new String[] {
                "Host",
                "User",
                "Select_priv",
                "Insert_priv",
                "Update_priv",
                "Delete_priv",
                "Create_priv",
                "Drop_priv",
                "Reload_priv",
                "Shutdown_priv",
                "Process_priv",
                "File_priv",
                "Grant_priv",
                "References_priv",
                "Index_priv",
                "Alter_priv",
                "Show_db_priv",
                "Super_priv",
                "Create_tmp_table_priv",
                "Lock_tables_priv",
                "Execute_priv",
                "Repl_slave_priv",
                "Repl_client_priv",
                "Create_view_priv",
                "Show_view_priv",
                "Create_routine_priv",
                "Alter_routine_priv",
                "Create_user_priv",
                "Event_priv",
                "Trigger_priv",
                "Create_tablespace_priv",
                "ssl_type",
                "ssl_cipher",
                "x509_issuer",
                "x509_subject",
                "max_questions",
                "max_updates",
                "max_connections",
                "max_user_connections",
                "plugin",
                "authentication_string",
                "password_expired",
                "password_last_changed",
                "password_lifetime",
                "account_locked",
            },
            "select host, user_name, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, NULL, NULL, NULL, NULL, grant_priv, NULL, index_priv, alter_priv,"
                +
                " NULL, NULL, NULL, NULL, NULL, NULL, NULL, create_view_priv, show_view_priv, NULL, NULL, create_user_priv, NULL, NULL, NULL, "
                +
                " NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL from "
                + MetaDbSchema.NAME + ".user_priv");

        defineView("tables_priv", new String[] {
                "Host",
                "Db",
                "User",
                "Table_name",
                "Grantor",
                "Timestamp",
                "Table_priv",
                "Column_priv",
            },
            "select host, db_name, user_name, table_name, grantor, gmt_modified, NULL, NULL from " + MetaDbSchema.NAME
                + ".table_priv");

        defineView("db", new String[] {
                "Host",
                "Db",
                "User",
                "Select_priv",
                "Insert_priv",
                "Update_priv",
                "Delete_priv",
                "Create_priv",
                "Drop_priv",
                "Grant_priv",
                "References_priv",
                "Index_priv",
                "Alter_priv",
                "Create_tmp_table_priv",
                "Lock_tables_priv",
                "Create_view_priv",
                "Show_view_priv",
                "Create_routine_priv",
                "Alter_routine_priv",
                "Execute_priv",
                "Event_priv",
                "Trigger_priv",
            },
            "select host, db_name, user_name, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, NULL, "
                +
                "index_priv, alter_priv, NULL, NULL, create_view_priv, NULL, NULL, NULL, NULL, NULL, NULL from "
                + MetaDbSchema.NAME + ".db_priv");
    }

    @Override
    protected void doDestroy() {
        // pass
        mysqlSchemaViews.clear();
    }

    @Override
    public SystemTableView.Row select(String viewName) {
        viewName = viewName.toLowerCase();
        Pair<List<String>, String> pair = mysqlSchemaViews.get(viewName);
        if (pair != null) {
            return new SystemTableView.Row(MysqlSchema.NAME, viewName, pair.getKey(), pair.getValue());
        } else {
            return null;
        }
    }

    @Override
    public void invalidate(String viewName) {
        throw new AssertionError();
    }

    @Override
    public boolean insert(String viewName, List<String> columnList, String viewDefinition, String definer,
                          String planString, String planType) {
        throw new AssertionError();
    }

    @Override
    public boolean replace(String viewName, List<String> columnList, String viewDefinition, String definer,
                           String planString, String planType) {
        throw new AssertionError();
    }

    @Override
    public boolean delete(String viewName) {
        throw new AssertionError();
    }

    private void defineView(String name, String[] columns, String definition) {
        mysqlSchemaViews.put(name,
            Pair.of(columns == null ? null : Arrays.stream(columns).collect(Collectors.toList()), definition));
    }

    private void defineVirtualView(VirtualViewType virtualViewType, String[] columns) {
        defineView(virtualViewType.name(), columns, virtualViewType.name());
    }
}

