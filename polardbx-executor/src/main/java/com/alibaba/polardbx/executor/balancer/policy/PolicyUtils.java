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

package com.alibaba.polardbx.executor.balancer.policy;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author moyi
 * @since 2021/08
 */
public class PolicyUtils {

    public static Map<String, GroupDetailInfoRecord> getGroupDetails(String schema) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(conn);
            List<GroupDetailInfoRecord> records =
                accessor.getGroupDetailInfoByInstIdAndDbName(InstIdUtil.getInstId(), schema);
            return records.stream().collect(Collectors.toMap(GroupDetailInfoRecord::getGroupName, x -> x));
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

}
