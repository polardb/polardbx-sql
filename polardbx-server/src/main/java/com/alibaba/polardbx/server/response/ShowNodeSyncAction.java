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

package com.alibaba.polardbx.server.response;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rpc.compatible.XDataSource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mengshi.sunmengshi 2015年5月12日 下午1:28:16
 * @since 5.1.0
 */
public class ShowNodeSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(ShowNodeSyncAction.class);

    private String schema;

    public ShowNodeSyncAction(String schema) {
        this.schema = schema;
    }

    public ShowNodeSyncAction() {

    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public ResultCursor sync() {

        MyRepository repo = (MyRepository) ExecutorContext.getContext(schema)
            .getRepositoryHolder()
            .get(GroupType.MYSQL_JDBC.toString());

        ArrayResultCursor result = new ArrayResultCursor("RULE");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("MASTER_READ_COUNT", DataTypes.LongType);
        result.addColumn("SLAVE_READ_COUNT", DataTypes.LongType);
        result.addColumn("MASTER_READ_PERCENT", DataTypes.StringType);
        result.addColumn("SLAVE_READ_PERCENT", DataTypes.StringType);

        List<Group> normalGroups = ExecutorContext.getContext(schema).getTopologyHandler().getMatrix().getGroups();
        buildShowNodeResults(repo, result, 0, normalGroups);
        List<Group> scaleOutGroups =
            ExecutorContext.getContext(schema).getTopologyHandler().getMatrix().getScaleOutGroups();
        buildShowNodeResults(repo, result, normalGroups.size(), scaleOutGroups);
        return result;
    }

    protected void buildShowNodeResults(MyRepository repo, ArrayResultCursor result, int i, List<Group> normalGroups) {
        List<String> grpNameList = new ArrayList<>();
        Map<String, TGroupDataSource> grpNameAndDsMap = new HashMap<>();
        for (Group group : normalGroups) {
            if (GroupType.MYSQL_JDBC.equals(group.getType())) {
                TGroupDataSource ds = repo.getDataSource(group.getName());
                if (ds != null) {
                    grpNameList.add(group.getName().toUpperCase());
                    grpNameAndDsMap.put(group.getName().toUpperCase(), ds);

                }
            }
        }
        for (int k = 0; k < grpNameList.size(); ++k) {

            Integer id = i++;
            String grpName = grpNameList.get(k);
            TGroupDataSource group = grpNameAndDsMap.get(grpName.toUpperCase());
            String name = group.getDbGroupKey();

            long masterReadCount = 0;
            long slaveReadCount = 0;

            String masterReadPercent;
            String slaveReadPercent;
            Map<TAtomDataSource, Weight> atoms = group.getAtomDataSourceWeights();

            java.text.NumberFormat percentFormat = java.text.NumberFormat.getPercentInstance();
            for (TAtomDataSource atom : atoms.keySet()) {
                Weight w = atoms.get(atom);

                final DataSource ds = atom.getDataSource();
                if (ds instanceof DruidDataSource) {
                    final DruidDataSource druidDS = (DruidDataSource) ds;

                    //TAtomDsDayStatisDO stat = new TAtomDsDayStatisDO();
//                try {
//                    stat = atom.getDataSource().getStatisticsHandle().getAtomTodayStat();
//                } catch (SQLException e) {
//                    logger.error("", e);
//                    continue;
//                }
                    if (w.w != 0) {
                        // master
                        masterReadCount += druidDS.getExecuteCount() - druidDS.getExecuteUpdateCount();

                    } else {
                        // slave
                        slaveReadCount += druidDS.getExecuteCount() - druidDS.getExecuteUpdateCount();
                    }
                } else if (ds instanceof XDataSource) {
                    final XDataSource xDS = (XDataSource) ds;
                    if (w.w != 0) {
                        // master
                        masterReadCount += xDS.getQueryCount().get();

                    } else {
                        // slave
                        slaveReadCount += xDS.getQueryCount().get();
                    }
                }
            }

            if (masterReadCount + slaveReadCount != 0) {
                masterReadPercent = percentFormat.format(masterReadCount / (float) (masterReadCount + slaveReadCount));
                slaveReadPercent = percentFormat.format(slaveReadCount / (float) (masterReadCount + slaveReadCount));
            } else {
                masterReadPercent = percentFormat.format(0.0);
                slaveReadPercent = percentFormat.format(0.0);
            }

            result
                .addRow(new Object[] {id, name, masterReadCount, slaveReadCount, masterReadPercent, slaveReadPercent});
        }
    }
}
