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

package com.alibaba.polardbx.optimizer.config.schema;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author dylan
 */
public class MetaDbSchema extends AbsSchema {

    public final static String NAME = "METADB";

    private static final MetaDbSchema INSTANCE = new MetaDbSchema();

    private LoadingCache<String, TableMeta> cache;

    private static final int cacheExpireTime = 24 * 60 * 1000; // 24h

    private MetaDbSchema() {
        cache = CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheExpireTime, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<String, TableMeta>() {

                @Override
                public TableMeta load(String tableName) throws Exception {
                    return getTable0(tableName);

                }
            });
    }

    public static MetaDbSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public TableMeta getTable(String tableName) {
        try {
            return cache.get(tableName);
        } catch (ExecutionException e) {
            cache.invalidate(tableName);
            throw new TddlNestableRuntimeException(e);
        }
    }

    private TableMeta getTable0(String tableName) {

        TableMeta tableMeta = null;
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            tableMeta =
                OptimizerContext.getContext(InformationSchema.NAME).getLatestSchemaManager()
                    .getTableMetaFromConnection(tableName, connection);

            if (tableMeta == null) {
                throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
            }
            tableMeta.setSchemaName(NAME);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        return tableMeta;
    }
}

