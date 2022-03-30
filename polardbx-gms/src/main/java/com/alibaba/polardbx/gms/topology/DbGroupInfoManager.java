package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.sql.SQLException;
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
            cache.put(schema, dbGroups);
            MetaDbLogUtil.META_DB_LOG.info(String.format("reload db group info for database %s: %s", schema, records));
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error("reload db group info from metadb failed: " + e);
        }
    }

    public DbGroupInfoRecord queryGroupInfo(String schema, String groupName) {
        Map<String, DbGroupInfoRecord> dbGroup = cache.get(schema);
        if (dbGroup == null) {
            return null;
        }
        return dbGroup.get(groupName);
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
            cache.remove(dbName);
        }

    }

    public void onDbTopologyListener(String dataId) {
        String schema = MetaDbDataIdBuilder.resolveDbTopologyDataId(dataId);
        reloadGroupsOfDb(schema);
    }

}
