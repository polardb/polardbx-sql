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

package com.alibaba.polardbx.executor.mdl;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;

/**
 * 线程请求锁时使用的对象，加锁成功后会将返回一个 MdlTicket
 *
 * @author chenmo.cm
 */
public final class MdlRequest {

    final MdlType type;
    final MdlDuration duration;
    final MdlKey key;
    final Long trxId;

    final String traceId;
    final ByteString sql;
    final String frontend;

    /**
     * 加锁成功后，返回的 ticket
     */
    MdlTicket ticket;

    /**
     * create MdlRequest
     *
     * @param trxId transaction id of caller
     * @param key mdl key
     * @param type mdl type
     * @param duration mdl duration
     */
    public MdlRequest(Long trxId, MdlKey key, MdlType type, MdlDuration duration) {
        this.trxId = trxId;
        this.key = key;
        this.type = type;
        this.duration = duration;
        this.traceId = null;
        this.sql = null;
        this.frontend = null;
    }

    public MdlRequest(Long trxId, MdlKey key, MdlType type, MdlDuration duration, String traceId, ByteString sql,
                      String frontend) {
        this.trxId = trxId;
        this.key = key;
        this.type = type;
        this.duration = duration;
        this.traceId = traceId;
        this.sql = sql;
        this.frontend = frontend;
    }

    /**
     * create MdlRequest with lower table name, type of MDL_SHARED_WRITE,
     * duration of MDL_TRANSACTION
     */
    public static MdlRequest getTransactionalDmlMdlRequest(@NotNull Long trxId, @NotNull String dbName,
                                                           @NotNull String tableName, @NotNull String traceId,
                                                           @NotNull ByteString sql, @NotNull String frontend) {
        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(dbName, tableName),
            MdlType.MDL_SHARED_WRITE,
            MdlDuration.MDL_TRANSACTION,
            traceId, sql, frontend);
    }

    /**
     * only for ossLoadData , get MDL_EXCLUSIVE mdl ,
     * downgrade to MDL_SHARED_WRITE later to prevent other ossLoadData
     */
    public static MdlRequest getTransactionalOssLoadDataMdlRequest(@NotNull Long trxId, @NotNull String dbName,
                                                                   @NotNull String tableName, @NotNull String traceId,
                                                                   @NotNull ByteString sql, @NotNull String frontend) {
        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(dbName, tableName),
            MdlType.MDL_EXCLUSIVE,
            MdlDuration.MDL_TRANSACTION,
            traceId, sql, frontend);
    }

    public MdlTicket getTicket() {
        return ticket;
    }

    public void setTicket(MdlTicket ticket) {
        this.ticket = ticket;
    }

    public MdlType getType() {
        return type;
    }

    public MdlDuration getDuration() {
        return duration;
    }

    public MdlKey getKey() {
        return key;
    }

    public Long getTrxId() {
        return trxId;
    }

    public String getTraceId() {
        return traceId;
    }

    public ByteString getSql() {
        return sql;
    }

    public String getFrontend() {
        return frontend;
    }

    @Override
    public String toString() {
        return "MdlRequest{" + "type=" + type + ", duration=" + duration + ", key=" + key + ", trxId='" + trxId + '\''
            + ", ticket=" + ticket + '}';
    }
}
