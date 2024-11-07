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

package com.alibaba.polardbx.optimizer.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupDdl;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class AlterTablePartitionHelper {

    public static boolean checkIfFromAlterIndexPartition(DDL alterTblDdl) {
        if (alterTblDdl instanceof AlterTable) {
            AlterTable alterTbl = (AlterTable) alterTblDdl;
            SqlNode alterNode = alterTbl.getSqlNode();
            if (alterNode instanceof SqlAlterTable) {
                SqlAlterTable alterTblAst = (SqlAlterTable) alterNode;
                boolean isFromAlterIdx = alterTblAst.isFromAlterIndexPartition();
                return isFromAlterIdx;
            }
        }
        return false;
    }

    public static SqlNode fetchGsiFullTableNameAstForAlterIndexPartition(DDL alterTblDdl) {
        if (alterTblDdl instanceof AlterTable) {
            AlterTable alterTbl = (AlterTable) alterTblDdl;
            SqlNode alterNode = alterTbl.getSqlNode();
            if (alterNode instanceof SqlAlterTable) {
                SqlAlterTable alterTblAst = (SqlAlterTable) alterNode;
                boolean isFromAlterIdx = alterTblAst.isFromAlterIndexPartition();
                SqlNode alterIndexNameNode = alterTblAst.getAlterIndexName();
                SqlNode tableNameOfAlterIndex = alterTblAst.getOriginTableName();
                if (isFromAlterIdx) {
                    SqlIdentifier tblAstOfAlterIdx = (SqlIdentifier) tableNameOfAlterIndex;
                    List<String> fullTblNameOfAlterIdxAst = tblAstOfAlterIdx.names;
                    TableMeta tblMetaOfTblOfIdx = getTableMetaByFullTblName(fullTblNameOfAlterIdxAst);
                    if (tblMetaOfTblOfIdx != null) {
                        SqlIdentifier alterIndexNameAst = (SqlIdentifier) alterIndexNameNode;
                        TableMeta gsiFullTblMeta =
                            getGsiTableMetaByPrimaryTblMetaAndIndexName(alterIndexNameAst, tblAstOfAlterIdx,
                                tblMetaOfTblOfIdx);
                        if (gsiFullTblMeta != null) {
                            String gsiDbName = gsiFullTblMeta.getSchemaName();
                            String gsiTblName = gsiFullTblMeta.getTableName();
                            List<String> gsiFullTableNames = new ArrayList<>();
                            gsiFullTableNames.add(gsiDbName);
                            gsiFullTableNames.add(gsiTblName);
                            SqlIdentifier gsiFullTblNameId = new SqlIdentifier(gsiFullTableNames, SqlParserPos.ZERO);
                            return gsiFullTblNameId;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static DDL fixAlterTableGroupDdlIfNeed(DDL alterTgDdl) {
        SqlNode alterAst = alterTgDdl.getAst();
        if (alterAst instanceof SqlAlterTableGroup) {
            SqlAlterTableGroup alterTgAst = (SqlAlterTableGroup) alterAst;
            boolean isAlterTgByTable = alterTgAst.isAlterByTable();
            if (isAlterTgByTable) {
                String targetTgName = fetchTableGroupNameByTableName(alterTgDdl);
                AlterTableGroupDdl ddl = (AlterTableGroupDdl) alterTgDdl;
                ddl.setTableGroupName(targetTgName);
                SqlIdentifier tgId = new SqlIdentifier(targetTgName, SqlParserPos.ZERO);
                ddl.setTableName(tgId);
                alterTgAst.setTargetTable(tgId);
                alterTgAst.setAlterByTable(false);
            }
        }
        return alterTgDdl;
    }

    public static String fetchTableGroupNameByTableName(DDL alterTgDdl) {
        String targetTgName = null;
        SqlNode alterAst = alterTgDdl.getAst();
        if (alterAst instanceof SqlAlterTableGroup) {
            SqlAlterTableGroup alterTgAst = (SqlAlterTableGroup) alterAst;
            boolean isAlterTgByTable = alterTgAst.isAlterByTable();
            boolean isAlterIndexTg = alterTgAst.isAlterIndexTg();
            if (isAlterTgByTable) {
                SqlIdentifier tblId = (SqlIdentifier) alterTgAst.getTableGroupName();
                TableMeta targetTblMeta = null;
                if (isAlterIndexTg) {
                    SqlIdentifier tblIdOfIndex = (SqlIdentifier) alterTgAst.getTblNameOfIndex();
                    List<String> fullTblNamesIdOfIndex = tblIdOfIndex.names;
                    TableMeta tblMetaOfIndex = getTableMetaByFullTblName(fullTblNamesIdOfIndex);
                    TableMeta gsiFullTblMeta =
                        getGsiTableMetaByPrimaryTblMetaAndIndexName(tblId, tblIdOfIndex, tblMetaOfIndex);
                    targetTblMeta = gsiFullTblMeta;
                } else {
                    List<String> fullTblNames = tblId.names;
                    TableMeta tbMeta = getTableMetaByFullTblName(fullTblNames);
                    targetTblMeta = tbMeta;
                }

                PartitionInfo partInfo = targetTblMeta.getPartitionInfo();
                Long tgId = partInfo.getTableGroupId();
                TableGroupConfig tgConfig =
                    OptimizerContext.getContext(targetTblMeta.getSchemaName()).getTableGroupInfoManager()
                        .getTableGroupConfigById(tgId);
                targetTgName = tgConfig.getTableGroupRecord().getTg_name();
            }
        }
        return targetTgName;
    }

    private static TableMeta getGsiTableMetaByPrimaryTblMetaAndIndexName(SqlIdentifier idxNameAst,
                                                                         SqlIdentifier tblNameAstOfIndex,
                                                                         TableMeta tblMetaOfIndex) {
        String dbNameOfTblOfIndex = tblMetaOfIndex.getSchemaName();
        String idxName = SQLUtils.normalizeNoTrim(idxNameAst.getLastName());
        String fullTblName = null;
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = tblMetaOfIndex.getGsiPublished();
        Map<String, GsiMetaManager.GsiIndexMetaBean> cciPublished = tblMetaOfIndex.getColumnarIndexPublished();
        if (gsiPublished != null) {
            String idxNameLowerCase = idxName.toLowerCase();
            // all gsi tbl name format is idxname + "_$xxxx";
            int targetLength = idxNameLowerCase.length() + 6;
            for (String gsiName : gsiPublished.keySet()) {
                if (gsiName.toLowerCase().startsWith(idxNameLowerCase) && gsiName.length() == targetLength) {
                    fullTblName = gsiName;
                    break;
                }
            }
            for (String cciName : cciPublished.keySet()) {
                if (cciName.toLowerCase().startsWith(idxNameLowerCase) && cciName.length() == targetLength) {
                    fullTblName = cciName;
                    break;
                }
            }
        }
        if (fullTblName == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Not found the index [%s] of table [%s]", idxName, tblNameAstOfIndex.toString()));
        }
        TableMeta fullTblMeta =
            OptimizerContext.getContext(dbNameOfTblOfIndex).getLatestSchemaManager().getTable(fullTblName);
        return fullTblMeta;
    }

    @NotNull
    private static TableMeta getTableMetaByFullTblName(List<String> fullTblNames) {
        String tbName = "";
        String dbName = "";
        if (fullTblNames.size() == 2) {
            dbName = SQLUtils.normalizeNoTrim(fullTblNames.get(0));
            tbName = SQLUtils.normalizeNoTrim(fullTblNames.get(1));
        } else if (fullTblNames.size() == 1) {
            tbName = SQLUtils.normalizeNoTrim(fullTblNames.get(0));
            dbName = OptimizerContext.getContext(dbName).getSchemaName();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Invalid table name found");
        }

        if (DbInfoManager.getInstance().getDbInfo(dbName) == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("No found database `%s`", dbName));
        }

        if (!DbInfoManager.getInstance().isNewPartitionDb(dbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "only database with mode='auto' support alter table/tablegroup partitions");
        }

        TableMeta tbMeta = OptimizerContext.getContext(dbName).getLatestSchemaManager().getTable(tbName);

        if (tbMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                String.format("No found database `%s`.`%s`", dbName, tbName));
        }
        return tbMeta;
    }
}
