/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_DUPLICATES;

public class ColumnarDuplicatesAccessor extends AbstractAccessor {
    private static final String COLUMNAR_DUPLICATES_TABLE = wrap(COLUMNAR_DUPLICATES);

    private static final String INSERT = "insert into " + COLUMNAR_DUPLICATES_TABLE
        + " (`engine`, `logical_schema`, `logical_table`, `partition_name`, `long_pk`, `bytes_pk`, `type`,"
        + " `before_file_id`, `before_pos`, `after_file_id`, `after_pos`, `extra`)"
        + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public void insert(Collection<ColumnarDuplicatesRecord> records) {
        try {
            final List<Map<Integer, ParameterContext>> params = new ArrayList<>();
            for (ColumnarDuplicatesRecord record : records) {
                final Map<Integer, ParameterContext> map = new HashMap<>();
                MetaDbUtil.setParameter(1, map, ParameterMethod.setString, record.engine);
                MetaDbUtil.setParameter(2, map, ParameterMethod.setString, record.logicalSchema);
                MetaDbUtil.setParameter(3, map, ParameterMethod.setString, record.logicalTable);
                if (null == record.partitionName) {
                    MetaDbUtil.setParameter(4, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(4, map, ParameterMethod.setString, record.partitionName);
                }
                if (null == record.longPk) {
                    MetaDbUtil.setParameter(5, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(5, map, ParameterMethod.setLong, record.longPk);
                }
                if (null == record.bytesPk) {
                    MetaDbUtil.setParameter(6, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(6, map, ParameterMethod.setBytes, record.bytesPk);
                }
                MetaDbUtil.setParameter(7, map, ParameterMethod.setString, record.type);
                if (null == record.beforeFileId) {
                    MetaDbUtil.setParameter(8, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(8, map, ParameterMethod.setLong, record.beforeFileId);
                }
                if (null == record.beforePos) {
                    MetaDbUtil.setParameter(9, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(9, map, ParameterMethod.setLong, record.beforePos);
                }
                if (null == record.afterFileId) {
                    MetaDbUtil.setParameter(10, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(10, map, ParameterMethod.setLong, record.afterFileId);
                }
                if (null == record.afterPos) {
                    MetaDbUtil.setParameter(11, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(11, map, ParameterMethod.setLong, record.afterPos);
                }
                if (null == record.extra) {
                    MetaDbUtil.setParameter(12, map, ParameterMethod.setNull1, null);
                } else {
                    MetaDbUtil.setParameter(12, map, ParameterMethod.setString, record.extra);
                }
                params.add(map);
            }

            final int[] inserts = MetaDbUtil.executeBatch(INSERT, params, connection);
            for (int insert : inserts) {
                if (insert != 1) {
                    throw new RuntimeException("ColumnarDuplicates partly inserts.");
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
