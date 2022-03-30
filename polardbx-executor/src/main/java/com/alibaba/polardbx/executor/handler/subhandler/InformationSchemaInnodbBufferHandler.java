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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.TransactionUtils;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbBufferPage;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbBufferPageLru;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbBufferPoolStats;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author youtianyu
 */
public class InformationSchemaInnodbBufferHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaInnodbBufferHandler.class);

    public InformationSchemaInnodbBufferHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbBufferPoolStats ||
            virtualView instanceof InformationSchemaInnodbBufferPage ||
            virtualView instanceof InformationSchemaInnodbBufferPageLru;
    }

    private String getQuery(VirtualView virtualView) {
        if (virtualView instanceof InformationSchemaInnodbBufferPoolStats) {
            return "select * from information_schema.innodb_buffer_pool_stats";
        } else if (virtualView instanceof InformationSchemaInnodbBufferPage) {
            return "select * from information_schema.innodb_buffer_page";
        } else if (virtualView instanceof InformationSchemaInnodbBufferPageLru) {
            return "select * from information_schema.innodb_buffer_page_lru";
        } else {
            throw new RuntimeException(
                "Invalid class of " + virtualView + " for " + InformationSchemaInnodbBufferHandler.class);
        }
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        List<RelDataTypeField> fieldList = virtualView.getRowType().getFieldList();

        int validRowCnt = 0;

        List<String> schemaNames = new ArrayList<>();
//        schemaNames.add(SystemDbHelper.INFO_SCHEMA_DB_NAME);
        schemaNames.add(executionContext.getSchemaName());

        Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(schemaNames);

        for (List<TGroupDataSource> groupDataSourceList : instId2GroupList.values()) {

            TGroupDataSource groupDataSource = groupDataSourceList.get(0);

            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                conn = groupDataSource.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(getQuery(virtualView));
                while (rs.next()) {
                    List<Object> newRow = new LinkedList<>();

                    int physicalBufferPoolId = rs.getInt(1);

                    // a dn can own 64 buffer pool instances at most
                    newRow.add((validRowCnt << 6) + physicalBufferPoolId);

                    for (int i = 2; i <= fieldList.size(); i++) {
                        newRow.add(rs.getObject(i));
                    }

                    cursor.addRow(newRow.toArray());
                }
            } catch (Throwable t) {
                logger.error(t);
            } finally {
                GeneralUtil.close(rs);
                GeneralUtil.close(stmt);
                GeneralUtil.close(conn);
                validRowCnt++;
            }
        }

        return cursor;
    }
}
