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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.stats.MatrixStatistics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowStc;

import java.util.Date;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalShowStcHandler extends HandlerCommon {
    public LogicalShowStcHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowStc showStc = (SqlShowStc) show.getNativeSqlNode();

        ArrayResultCursor result = new ArrayResultCursor("STC");
        result.addColumn("DBNAME", DataTypes.StringType);
        result.addColumn("MYSQLADDR", DataTypes.StringType);
        result.addColumn("APPNAME", DataTypes.StringType);
        result.addColumn("GROUPNAME", DataTypes.StringType);
        result.addColumn("ATOMNAME", DataTypes.StringType);
        result.addColumn("READCOUNT", DataTypes.StringType);//从启动到目前为止所接收的到读请求数
        result.addColumn("WRITECOUNT", DataTypes.StringType);//从启动到目前为止所接收的到写请求数
        result.addColumn("TOTALCOUNT", DataTypes.StringType);//从启动到目前为止所接收的到总请求数
        result.addColumn("READTIMECOST", DataTypes.StringType);//从启动到目前为止所有读请求的执行用时, 单位: ms毫秒
        result.addColumn("WRITETIMECOST", DataTypes.StringType);//从启动到目前为止所有写请求的执行用时，单位: ms毫秒
        result.addColumn("TOTALTIMECOST", DataTypes.StringType);//从启动到目前为止所有总请求的执行用时，单位: ms毫秒
        result.addColumn("CONNERRCOUNT", DataTypes.StringType);//从启动到目前为止所接收的到连接错误数目
        result.addColumn("SQLERRCOUNT", DataTypes.StringType);//从启动到目前为止所接收SQL错误数目
        result.addColumn("SQLLENGTH", DataTypes.StringType);//当前Atom执行的SQL 总长度
        result.addColumn("ROWS", DataTypes.StringType);//当前Atom反馈的总行数

        if (showStc.isFull()) {
            List<List<Object>> list = MatrixStatistics.getStcInfo();
            for (List<Object> obs : list) {
                result.addRow(obs.toArray());
            }
        } else {
            result.addColumn("START_TIME", DataTypes.StringType);//当前Atom反馈的总行数
            List<List<Object>> list = MatrixStatistics.getCurrentStcInfo();
            for (List<Object> obs : list) {
                obs.add(new Date(MatrixStatistics.currentRoundTime));
                result.addRow(obs.toArray());
            }
        }
        return result;
    }
}
