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

package com.alibaba.polardbx.gms.metadb.misc;

import com.google.common.base.Joiner;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ReadWriteLockAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteLockAccessor.class);

    public static final String READ_WRITE_LOCK_TABLE = wrap(GmsSystemTables.READ_WRITE_LOCK);

    private static final String INSERT_DATA =
        "insert into " + READ_WRITE_LOCK_TABLE
            + "(`schema_name`, `owner`, `resource`, `type`) "
            + "values (?, ?, ?, ?)";

    private static final String SELECT_FULL =
        "select `schema_name`, `owner`, `resource`, `type`, `gmt_created`, `gmt_modified` ";

    private static final String FROM_TABLE = " from " + READ_WRITE_LOCK_TABLE;

    private static final String WHERE_OWNER = " where `owner` = ?";

    private static final String WHERE_SCHEMA_NAME = " where `schema_name` = ?";

    private static final String WHERE_RESOURCE = " where `resource` = ?";

    private static final String WHERE_RESOURCE_IN_TEMPLATE = " where `resource` in (%s) ";

    private static final String WITH_RESOURCE = " and `resource` = ?";

    private static final String WITH_TYPE = " and `type` = ?";

    private static final String FOR_UPDATE = " for update";

    private static final String IN_SHARE_MODE = " lock in share mode";

    private static final String SELECT_BY_OWNER = SELECT_FULL + FROM_TABLE + WHERE_OWNER;

    private static final String SELECT_BY_RESOURCE_TYPE = SELECT_FULL + FROM_TABLE + WHERE_RESOURCE + WITH_TYPE;

    private static final String SELECT_BY_RESOURCE_TYPE_FOR_UPDATE =
        SELECT_FULL + FROM_TABLE + WHERE_RESOURCE + WITH_TYPE + FOR_UPDATE;

    private static final String SELECT_BY_RESOURCE_TYPE_IN_SHARE_MODE =
        SELECT_FULL + FROM_TABLE + WHERE_RESOURCE + WITH_TYPE + IN_SHARE_MODE;

    private static final String SELECT_BY_RESOURCE = SELECT_FULL + FROM_TABLE + WHERE_RESOURCE;

    private static final String SELECT_BY_RESOURCE_LIST_TEMPLATE =
        SELECT_FULL + FROM_TABLE + WHERE_RESOURCE_IN_TEMPLATE;

    private static final String DELETE_BY_SCHEMA_NAME = "delete from " + READ_WRITE_LOCK_TABLE + WHERE_SCHEMA_NAME;

    private static final String DELETE_BY_OWNER = "delete from " + READ_WRITE_LOCK_TABLE + WHERE_OWNER;

    private static final String DELETE_BY_OWNER_RESOURCE_TYPE =
        "delete from " + READ_WRITE_LOCK_TABLE + WHERE_OWNER + WITH_RESOURCE + WITH_TYPE;

    public int insert(List<ReadWriteLockRecord> recordList) {
        try {

            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(e -> e.buildParams()).collect(Collectors.toList());
            int[] r = MetaDbUtil.insert(INSERT_DATA, paramsBatch, connection);
            return Arrays.stream(r).sum();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<ReadWriteLockRecord> query(String owner) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {owner});

            List<ReadWriteLockRecord> records =
                MetaDbUtil.query(SELECT_BY_OWNER, params, ReadWriteLockRecord.class, connection);

            if (records != null) {
                return records;
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_TABLE, "query from", e);
        }
    }

    public Optional<ReadWriteLockRecord> query(String resource, String type) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {resource, type});

            List<ReadWriteLockRecord> records =
                MetaDbUtil.query(SELECT_BY_RESOURCE_TYPE, params, ReadWriteLockRecord.class, connection);

            if (CollectionUtils.isNotEmpty(records)) {
                return Optional.of(records.get(0));
            }
            return Optional.empty();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_TABLE, "query from", e);
        }
    }

    public Optional<ReadWriteLockRecord> queryInShareMode(String resource, String type) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {resource, type});

            List<ReadWriteLockRecord> records =
                MetaDbUtil
                    .query(SELECT_BY_RESOURCE_TYPE_IN_SHARE_MODE, params, ReadWriteLockRecord.class, connection);

            if (CollectionUtils.isNotEmpty(records)) {
                return Optional.of(records.get(0));
            }
            return Optional.empty();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_TABLE, "query from", e);
        }
    }

    public List<ReadWriteLockRecord> queryReader(String resource) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {resource});

            List<ReadWriteLockRecord> records =
                MetaDbUtil.query(SELECT_BY_RESOURCE, params, ReadWriteLockRecord.class, connection);

            if (records != null) {
                return records;
            }
            return new ArrayList<>();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_TABLE, "query from", e);
        }
    }

    public List<ReadWriteLockRecord> query(Set<String> resourceList, boolean forUpdate) {
        if (CollectionUtils.isEmpty(resourceList)) {
            return new ArrayList<>();
        }
        try {
            resourceList = resourceList.stream().map(e -> "'" + e + "'").collect(Collectors.toSet());
            String resourceInList = Joiner.on(",").skipNulls().join(resourceList);
            String sql = String.format(SELECT_BY_RESOURCE_LIST_TEMPLATE, resourceInList);
            if (forUpdate) {
                sql += FOR_UPDATE;
            }

            List<ReadWriteLockRecord> records =
                MetaDbUtil.query(sql, ReadWriteLockRecord.class, connection);

            if (records != null) {
                return records;
            }
            return new ArrayList<>();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + READ_WRITE_LOCK_TABLE, "query from", e);
        }
    }

    public int deleteByOwner(String owner) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {owner});
            return MetaDbUtil.delete(DELETE_BY_OWNER, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + READ_WRITE_LOCK_TABLE + " for owner " + owner, "delete from",
                e);
        }
    }

    public int deleteByOwnerAndResourceAndType(String owner, String resource, String type) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(16);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, resource);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, type);

            return MetaDbUtil.delete(DELETE_BY_OWNER_RESOURCE_TYPE, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + READ_WRITE_LOCK_TABLE + " for resource " + resource,
                "delete from",
                e);
        }
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + READ_WRITE_LOCK_TABLE + " for schemaName: " + schemaName,
                "delete from",
                e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            READ_WRITE_LOCK_TABLE, e.getMessage());
    }

}
