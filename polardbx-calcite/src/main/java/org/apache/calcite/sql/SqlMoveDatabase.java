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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version 1.0
 */
public class SqlMoveDatabase extends SqlCreate {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("MOVE DATABASE", SqlKind.MOVE_DATABASE);
    private Map<String, List<String>> storageGroups;
    private Map<String, Map<String, List<String>>> logicalDbStorageGroups;
    private String currentStorageInstId;
    private String currentSourceGroupKey;
    private String currentTargetGroupKey;
    private boolean isCleanUpCommand;
    private final static String VIRTUAL_STORAGE_ID = "VIRTUAL_STORAGE_ID";

    public SqlMoveDatabase(SqlParserPos pos, Map<String, List<String>> storageGroups,
                           boolean isCleanUpCommand) {
        super(OPERATOR, pos, false, false);
        this.storageGroups = storageGroups;
        String firstGroup = "";
        for (Map.Entry<String, List<String>> entry : storageGroups.entrySet()) {
            firstGroup = entry.getValue().get(0);
            this.currentStorageInstId = entry.getKey();
        }
        this.name = new SqlIdentifier(firstGroup, SqlParserPos.ZERO);
        this.isCleanUpCommand = isCleanUpCommand;
        if (!isCleanUpCommand) {
            this.currentSourceGroupKey = firstGroup;
            this.currentTargetGroupKey = GroupInfoUtil.buildScaloutGroupName(firstGroup);
        }
    }

    public Map<String, List<String>> getStorageGroups() {
        return storageGroups;
    }

    public void setStorageGroups(Map<String, List<String>> storageGroups) {
        this.storageGroups = storageGroups;
    }

    public void setCurrentStorageInstId(String currentStorageInstId) {
        this.currentStorageInstId = currentStorageInstId;
    }

    public void setCleanUpGroups(String groupName) {
        Map<String, List<String>> toBeCleanGroups = new HashMap<>();
        toBeCleanGroups.put(VIRTUAL_STORAGE_ID, ImmutableList.of(GroupInfoUtil.buildScaloutGroupName(groupName)));
        this.storageGroups = toBeCleanGroups;
    }

    public String getCurrentStorageInstId() {
        return currentStorageInstId;
    }

    public String getCurrentSourceGroupKey() {
        return currentSourceGroupKey;
    }

    public void setCurrentSourceGroupKey(String currentSourceGroupKey) {
        this.currentSourceGroupKey = currentSourceGroupKey;
    }

    public String getCurrentTargetGroupKey() {
        return currentTargetGroupKey;
    }

    public void setCurrentTargetGroupKey(String currentTargetGroupKey) {
        this.currentTargetGroupKey = currentTargetGroupKey;
    }

    public String getCurrentMoveTaskSql(Map<String, Object> hints) {
        if (GeneralUtil.isEmpty(hints)) {
            return "MOVE DATABASE " + currentSourceGroupKey + " TO '" + currentStorageInstId + "'";
        } else {
            StringBuffer sb = new StringBuffer();
            sb.append("MOVE DATABASE /*+TDDL: cmd_extra(");
            boolean firstKV = true;
            for (Map.Entry<String, Object> entry : hints.entrySet()) {
                if (!firstKV) {
                    sb.append(",");
                }
                firstKV = false;
                sb.append(entry.getKey());
                sb.append("=");
                sb.append(entry.getValue().toString());
            }
            sb.append(")*/ " + currentSourceGroupKey + " TO '" + currentStorageInstId + "'");
            return sb.toString();
        }
    }

    public boolean isCleanUpCommand() {
        return isCleanUpCommand;
    }

    public int getMoveGroupCount() {
        final AtomicInteger count = new AtomicInteger(0);
        storageGroups.forEach(((k, v) -> count.addAndGet(v.size())));
        return count.get();
    }

    public String getSql() {
        SqlPrettyWriter writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
        unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }

    public Map<String, Map<String, List<String>>> getLogicalDbStorageGroups() {
        return logicalDbStorageGroups;
    }

    public void setLogicalDbStorageGroups(
        Map<String, Map<String, List<String>>> logicalDbStorageGroups) {
        this.logicalDbStorageGroups = logicalDbStorageGroups;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("MOVE");
        writer.sep("DATABASE");
        if (isCleanUpCommand) {
            writer.sep("CLEAN");
        }

        boolean firstKey = true;
        for (Map.Entry<String, List<String>> entry : storageGroups.entrySet()) {
            boolean firstGroup = true;
            if (!firstKey) {
                writer.sep(",");
            }
            for (String id : entry.getValue()) {
                if (firstGroup) {
                    firstGroup = false;
                    if (!isCleanUpCommand) {
                        writer.sep("(");
                    }

                } else {
                    writer.sep(",");
                }
                writer.sep(id.toUpperCase());
            }
            if (!isCleanUpCommand) {
                writer.sep(")");
                writer.sep("TO");
                writer.sep("'" + entry.getKey().toUpperCase() + "'");
            } else {
                break;
            }
            firstKey = false;
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.MOVE_DATABASE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name);
    }
}
