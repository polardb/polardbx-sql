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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class DefaultDBInfo {

    public Map<String, ShardGroupInfo> groupAndPhyDbMaps = new HashMap<>();

    public static class ShardGroupInfo {
        public String dbName;
        public Map<String, String> groupAndPhyDbMaps = new HashMap<>();
        public String defaultDbIndex;
    }

    private static DefaultDBInfo defaultDBInfo = new DefaultDBInfo();

    private boolean inited = false;

    public DefaultDBInfo() {

    }

    public boolean isInited() {
        return inited;
    }

    public static DefaultDBInfo getInstance() {
        if (!defaultDBInfo.isInited()) {
            synchronized (defaultDBInfo) {
                if (!defaultDBInfo.isInited()) {
                    defaultDBInfo.init();
                }
            }
        }
        return defaultDBInfo;
    }

    private void init() {
        Pair<String, ShardGroupInfo> rets1 =
            getShardGroupListByMetaDb(PropertiesUtil.polardbXShardingDBName1(), false);
        Pair<String, ShardGroupInfo> rets2 =
            getShardGroupListByMetaDb(PropertiesUtil.polardbXShardingDBName2(), false);

        Pair<String, ShardGroupInfo> rets3 =
            getShardGroupListByMetaDb(PropertiesUtil.polardbXAutoDBName1(), true);
        Pair<String, ShardGroupInfo> rets4 =
            getShardGroupListByMetaDb(PropertiesUtil.polardbXAutoDBName2(), true);

        groupAndPhyDbMaps.put(rets1.getKey(), rets1.getValue());
        groupAndPhyDbMaps.put(rets2.getKey(), rets2.getValue());
        groupAndPhyDbMaps.put(rets3.getKey(), rets1.getValue());
        groupAndPhyDbMaps.put(rets4.getKey(), rets2.getValue());
    }

    public Pair<String, ShardGroupInfo> getShardGroupListByMetaDb(String dbName, boolean usePart) {
        if (groupAndPhyDbMaps.containsKey(dbName)) {
            return new Pair<>(dbName, groupAndPhyDbMaps.get(dbName));
        }
        Pair<String, ShardGroupInfo> grpNamePhyDbNames = null;
        try (Connection metaDbConn = ConnectionManager.getInstance().getDruidMetaConnection()) {
            JdbcUtil.useDb(metaDbConn, PropertiesUtil.getMetaDB);
            try (Statement stmt = metaDbConn.createStatement()) {
                stmt.execute(String
                    .format("select group_name, phy_db_name from db_group_info where db_name='%s'",
                        dbName));
                Map<String, String> grpNamePhyDbName = new HashMap<>();
                try (ResultSet rs = stmt.getResultSet()) {
                    ShardGroupInfo groupInfos = new ShardGroupInfo();
                    String defaultDb = "";
                    while (rs.next()) {
                        String grpName = rs.getString("group_name");
                        String phyDbName = rs.getString("phy_db_name");
                        grpNamePhyDbName.put(grpName, phyDbName);

                        if (usePart) {
                            if (grpName.toLowerCase().contains("p00000")) {
                                defaultDb = grpName;
                            }
                        } else {
                            if (grpName.toLowerCase().contains("_single")) {
                                defaultDb = grpName;
                            }
                        }

                    }

                    groupInfos.dbName = dbName;
                    groupInfos.groupAndPhyDbMaps = grpNamePhyDbName;
                    groupInfos.defaultDbIndex = defaultDb;
                    grpNamePhyDbNames = new Pair<>(dbName, groupInfos);
                } catch (Throwable ex) {
                    throw ex;
                }
            } catch (Throwable ex) {
                throw ex;
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
        return grpNamePhyDbNames;
    }
}
