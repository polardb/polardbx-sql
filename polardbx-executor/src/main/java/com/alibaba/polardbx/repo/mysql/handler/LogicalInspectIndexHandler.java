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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalInspectIndex;
import com.alibaba.polardbx.repo.mysql.InspectIndex.InspectIndexInfo;
import com.alibaba.polardbx.repo.mysql.InspectIndex.TableIndexInspector;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInspectIndex;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalInspectIndexHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowConvertTableHandler.class);

    public LogicalInspectIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalInspectIndex logicalInspectIndex = (LogicalInspectIndex) logicalPlan;
        SqlInspectIndex sqlInspectIndex = (SqlInspectIndex) logicalInspectIndex.relDdl.getSqlNode();
        String schema = executionContext.getSchemaName();

        Set<String> tables = new TreeSet<>(String::compareToIgnoreCase);
        if (!sqlInspectIndex.isFromTable()) {
            tables = TableIndexInspector.queryTableNamesFromSchema(schema);
        } else {
            tables.add(sqlInspectIndex.getTableName().toString());
        }

        ArrayResultCursor cursor;
        if (sqlInspectIndex.isFull()) {
            cursor = buildFullCursor();
        } else {
            cursor = buildCursor();
        }

        Map<String, TableIndexInspector> allInspectors = TableIndexInspector.createTableInspectorsInSchema(schema);

        String advice1 = "";
        String advice2 = "";
        for (String table : tables) {
            if (!allInspectors.containsKey(table)) {
                continue;
            }
            TableIndexInspector inspector = allInspectors.get(table);
            if (sqlInspectIndex.getMode() == null || sqlInspectIndex.getMode().equalsIgnoreCase("STATIC")) {
                inspector.inspectIndex();
            } else if (sqlInspectIndex.getMode().equalsIgnoreCase("DYNAMIC")) {
                inspector.inspectUseFrequency()
                    .inspectAccessTime()
                    .inspectDiscrimination()
                    .inspectHotSpot();
            } else if (sqlInspectIndex.getMode().equalsIgnoreCase("MIXED")) {
                inspector.inspectUseFrequency()
                    .inspectAccessTime()
                    .inspectDiscrimination()
                    .inspectHotSpot()
                    .inspectIndex();
            }

            if (sqlInspectIndex.isFull()) {
                addFullRowsByInspectInfo(schema, inspector, cursor);
            } else {
                Pair<String, String> advice = genRowByInspectInfo(inspector);
                advice1 = advice1 + advice.getKey();
                advice2 = advice2 + advice.getValue();
            }
        }

        if (!sqlInspectIndex.isFull()) {
            cursor.addRow(
                new Object[] {
                    advice1,
                    advice2
                }
            );
        }

        return cursor;
    }

    protected ArrayResultCursor buildFullCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("INSPECT_INDEX_INFO");
        cursor.addColumn("SCHEMA", DataTypes.StringType);
        cursor.addColumn("TABLE", DataTypes.StringType);
        cursor.addColumn("INDEX", DataTypes.StringType);
        cursor.addColumn("INDEX_TYPE", DataTypes.StringType);
        cursor.addColumn("INDEX_COLUMN", DataTypes.StringType);
        cursor.addColumn("COVERING_COLUMN", DataTypes.StringType);
        cursor.addColumn("USE_COUNT", DataTypes.LongType);
        cursor.addColumn("LAST_ACCESS_TIME", DataTypes.TimestampType);
        cursor.addColumn("DISCRIMINATION", DataTypes.DoubleType);
        cursor.addColumn("PROBLEM", DataTypes.StringType);
        cursor.addColumn("ADVICE (STEP1)", DataTypes.StringType);
        cursor.addColumn("ADVICE (STEP2)", DataTypes.StringType);
        cursor.initMeta();
        return cursor;
    }

    protected ArrayResultCursor buildCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("INSPECT_INDEX_INFO");
        cursor.addColumn("ADVICE (STEP1)", DataTypes.StringType);
        cursor.addColumn("ADVICE (STEP2)", DataTypes.StringType);
        cursor.initMeta();
        return cursor;
    }

    protected Pair<String, String> genRowByInspectInfo(TableIndexInspector inspector) {
        Map<String, InspectIndexInfo> infos = inspector.getGlobalIndexInspectInfo();
        String advice1Str = "";
        String advice2Str = "";
        for (Map.Entry<String, InspectIndexInfo> entry : infos.entrySet()) {
            InspectIndexInfo info = entry.getValue();
            info.generateAdvice();
            if (!info.invisibleIndexAdvice.isEmpty()) {
                advice1Str = String.join("\n", advice1Str, String.join(" \n", info.invisibleIndexAdvice));
            }
            if (!info.advice.isEmpty()) {
                advice2Str = String.join("\n", advice2Str, String.join(" \n", info.advice));
            }
        }

        Map<String, InspectIndexInfo> lsiInfos = inspector.getLocalIndexInspectInfo();
        for (Map.Entry<String, InspectIndexInfo> entry : lsiInfos.entrySet()) {
            InspectIndexInfo info = entry.getValue();
            info.generateAdvice();
            if (!info.invisibleIndexAdvice.isEmpty()) {
                advice1Str = String.join("\n", advice1Str, String.join(" \n", info.invisibleIndexAdvice));
            }
            if (!info.advice.isEmpty()) {
                advice2Str = String.join("\n", advice2Str, String.join(" \n", info.advice));
            }
        }

        return Pair.of(advice1Str, advice2Str);
    }

    protected void addFullRowsByInspectInfo(String schema, TableIndexInspector inspector, ArrayResultCursor cursor) {
        Map<String, InspectIndexInfo> infos = inspector.getGlobalIndexInspectInfo();
        for (Map.Entry<String, InspectIndexInfo> entry : infos.entrySet()) {
            String indexName = entry.getKey();
            InspectIndexInfo info = entry.getValue();

            info.generateAdvice();
            if (info.problem.isEmpty()) {
                continue;
            }
            String problemStr = info.problem.values().isEmpty() ? "None" : String.join(" \n",
                info.problem.values().stream().filter(s -> !StringUtil.isNullOrEmpty(s)).collect(Collectors.toList()));
            String advice1Str =
                info.invisibleIndexAdvice.isEmpty() ? "None" : String.join(" \n", info.invisibleIndexAdvice);
            String advice2Str = info.advice.isEmpty() ? "None" : String.join(" \n", info.advice);
            cursor.addRow(
                new Object[] {
                    schema,
                    info.tableName,
                    info.getLogicalGsiName(),
                    info.unique ? "UNIQUE GLOBAL INDEX" : "GLOBAL INDEX",
                    String.join(",", info.indexColumns),
                    String.join(",", info.coveringColumns),
                    info.useCount,
                    info.accessTime,
                    String.format("%.1f", info.rowDiscrimination),
                    problemStr,
                    advice1Str,
                    advice2Str
                }
            );
        }

        Map<String, InspectIndexInfo> lsiInfos = inspector.getLocalIndexInspectInfo();
        for (Map.Entry<String, InspectIndexInfo> entry : lsiInfos.entrySet()) {
            String indexName = entry.getKey();
            InspectIndexInfo info = entry.getValue();
            info.generateAdvice();
            if (info.problem.isEmpty()) {
                continue;
            }
            String problemStr = info.problem.values().isEmpty() ?
                "None" : String.join(" \n", info.problem.values());
            String advice1Str = "None";
            String advice2Str = info.advice.isEmpty() ?
                "None" : String.join(" \n", info.advice);
            cursor.addRow(
                new Object[] {
                    schema,
                    info.tableName,
                    indexName,
                    "LOCAL INDEX",
                    String.join(",", info.indexColumns),
                    null,
                    null,
                    null,
                    null,
                    problemStr,
                    advice1Str,
                    advice2Str
                }
            );
        }
    }
}
