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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.List;

/**
 * utils to get the tablegroupInfo from metadb.
 *
 * @author luoyanxin
 */
public class JoinGroupUtils {

    public static void deleteJoinGroupInfoBySchema(String schemaName, Connection metadb) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();

        joinGroupInfoAccessor.setConnection(metadb);
        joinGroupTableDetailAccessor.setConnection(metadb);
        joinGroupInfoAccessor.deleteJoinGroupInfoBySchema(schemaName);
        joinGroupTableDetailAccessor.deleteJoinGroupTableDetailBySchema(schemaName);
    }

    public static void deleteEmptyJoinGroupInfo(String schemaName, String joinGroupName, boolean ifExists,
                                                Connection metadb) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();

        joinGroupInfoAccessor.setConnection(metadb);
        joinGroupTableDetailAccessor.setConnection(metadb);

        JoinGroupInfoRecord record =
            joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroupName, true);
        if (record != null) {
            List<JoinGroupTableDetailRecord> joinGroupTableDetailRecords =
                joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaJoinGroupId(schemaName, record.id, true);
            if (GeneralUtil.isNotEmpty(joinGroupTableDetailRecords)) {
                throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_EMPTY,
                    " the joingroup:" + joinGroupName
                        + " can not be deleted because it's not empty");
            }
            joinGroupInfoAccessor.deleteJoinGroupInfoByName(schemaName, joinGroupName);
        } else if (!ifExists) {
            throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_NOT_EXISTS,
                " the joingroup:" + joinGroupName
                    + " is not exists");
        }
    }

    public static List<JoinGroupTableDetailRecord> getJoinGroupDetailByName(String schemaName, String joinGroupName,
                                                                            Connection metadb) {
        return MetaDbUtil.queryMetaDbWrapper(metadb, (conn) -> {
            JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
            JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();

            joinGroupInfoAccessor.setConnection(conn);
            joinGroupTableDetailAccessor.setConnection(conn);

            JoinGroupInfoRecord record =
                joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroupName, true);
            if (record != null) {
                List<JoinGroupTableDetailRecord> joinGroupTableDetailRecords =
                    joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaJoinGroupId(schemaName, record.id, false);
                return joinGroupTableDetailRecords;
            }
            return null;
        });
    }

    public static JoinGroupInfoRecord getJoinGroupInfoByTable(String schemaName, String tableName,
                                                              Connection metaDbConn) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
            JoinGroupTableDetailAccessor joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();
            joinGroupInfoAccessor.setConnection(conn);
            joinGroupTableDetailAccessor.setConnection(conn);
            JoinGroupTableDetailRecord joinGroupTableDetailRecord =
                joinGroupTableDetailAccessor.getJoinGroupDetailBySchemaTableName(schemaName, tableName);
            if (joinGroupTableDetailRecord == null) {
                return null;
            }
            return joinGroupInfoAccessor.getJoinGroupInfoById(schemaName,
                joinGroupTableDetailRecord.joinGroupId);
        });
    }

    public static JoinGroupInfoRecord getJoinGroupInfoByJoinGroupName(String schemaName, String joinGroupName) {
        return MetaDbUtil.queryMetaDbWrapper(null, (conn) -> {
            JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
            joinGroupInfoAccessor.setConnection(conn);

            return joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroupName, false);
        });
    }

    public static List<JoinGroupInfoRecord> getAllJoinGroupInfos(Connection metaDbConn) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
            joinGroupInfoAccessor.setConnection(conn);

            return joinGroupInfoAccessor.getJoinGroupInfos();
        });
    }

    public static List<String> getDistinctSchemaNames(Connection metaDbConn) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
            joinGroupInfoAccessor.setConnection(conn);

            return joinGroupInfoAccessor.getDistinctSchemaNames();
        });
    }
}
