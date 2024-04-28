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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DbGroupInfoManager maintains group information
 *
 * @author moyi
 * @since 2021/10
 */
public class DbGroupInfoManager extends AbstractLifecycle {

    private final static DbGroupInfoManager instance = new DbGroupInfoManager();

    //!!!NOTE!!! the key(schema name) should convert to lower case
    private final static Map<String, Map<String, DbGroupInfoRecord>> cache = new ConcurrentHashMap<>();

    public static DbGroupInfoManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    @Override
    protected void doInit() {
        for (DbInfoRecord dbInfo : DbInfoManager.getInstance().getDbInfoList()) {
            reloadGroupsOfDb(dbInfo.dbName);
        }
    }

    private void reloadGroupsOfDb(String schema) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);

            List<DbGroupInfoRecord> records = dbGroupInfoAccessor.queryDbGroupByDbName(schema);
            Map<String, DbGroupInfoRecord> dbGroups =
                records.stream().collect(Collectors.toMap(x -> x.groupName, x -> x));

            // replace existed cache
            schema = schema.toLowerCase();
            cache.put(schema, dbGroups);
            MetaDbLogUtil.META_DB_LOG.info(String.format("reload db group info for database %s: %s", schema, records));
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error("reload db group info from metadb failed: " + e);
        }
    }

    public DbGroupInfoRecord queryGroupInfo(String schema, String groupName) {
        schema = schema.toLowerCase();
        Map<String, DbGroupInfoRecord> dbGroup = cache.get(schema);
        if (dbGroup == null) {
            return null;
        }
        return dbGroup.get(groupName);
    }

    public List<DbGroupInfoRecord> queryGroupInfoBySchema(String schema) {
        schema = schema.toLowerCase();
        Map<String, DbGroupInfoRecord> dbGroup = cache.get(schema);
        if (dbGroup == null) {
            return null;
        }
        return new ArrayList<>(dbGroup.values());
    }

    public static boolean isNormalGroup(Group group) {
        return isNormalGroup(group.getSchemaName(), group.getName());
    }

    public static boolean isNormalGroup(String schema, String groupName) {
        DbGroupInfoRecord record = getInstance().queryGroupInfo(schema, groupName);
        return record != null && record.isNormal();
    }

    public static boolean isVisibleGroup(Group group) {
        return isVisibleGroup(group.getSchemaName(), group.getName());
    }

    public static boolean isVisibleGroup(String schema, String groupName) {
        DbGroupInfoRecord record = getInstance().queryGroupInfo(schema, groupName);
        return record != null && record.isVisible();
    }

    public void onDbInfoChange(Map<String, DbInfoRecord> added, Map<String, DbInfoRecord> removed) {
        for (String dbName : added.keySet()) {
            reloadGroupsOfDb(dbName);
        }
        for (String dbName : removed.keySet()) {
            dbName = dbName.toLowerCase();
            cache.remove(dbName);
        }

    }

    public void onDbTopologyListener(String dataId) {
        String schema = MetaDbDataIdBuilder.resolveDbTopologyDataId(dataId);
        reloadGroupsOfDb(schema);
    }

    public void unbindListenersForAllGroupsOfDb(String schema) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);
            String instId = InstIdUtil.getInstId();
            List<DbGroupInfoRecord> records = dbGroupInfoAccessor.queryDbGroupByDbName(schema);
            for (int i = 0; i < records.size(); i++) {
                DbGroupInfoRecord dbGrp = records.get(i);
                String grpName = dbGrp.groupName;
                String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, schema, grpName);
                MetaDbConfigManager.getInstance().unbindListener(grpDataId);
            }
            MetaDbLogUtil.META_DB_LOG.info(
                String.format("unbind all listeners for groups info for database %s", schema));
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(
                String.format("failed to unbind all listeners for groups of database %s: ", schema) + e);
        }
    }

}
