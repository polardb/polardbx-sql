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

package com.alibaba.polardbx.executor.ddl.job.factory.storagepool;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupExtRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.List;

public class StoragePoolUtils {
    //    public static List<StorageInfoRecord> getStorageInfo() {
//
//        // query metadb for physical group and all partition-groups
//        List<StorageInfoRecord> storageInfoRecords;
//        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
//            String instId = InstIdUtil.getInstId();
//            storageInfoRecords = queryStorageInfo(conn, instId);
//        } catch (Throwable ex) {
//            MetaDbLogUtil.META_DB_LOG.error(ex);
//            throw GeneralUtil.nestedException(ex);
//        }
//    }
    public static String RECYCLE_STORAGE_POOL = "_recycle";
    public static String DEFAULT_STORAGE_POOL = "_default";

    public static String ALL_STORAGE_POOL = "__all_storage_pool";
    public static String LOCK_PREFIX = "lock_storage_pool";

    public static String FULL_LOCK_NAME = "full_";

    public String formatDnList(String dnList) {
        return dnList.replaceAll("^\"|\"$|^'|'$", "");
    }

}
