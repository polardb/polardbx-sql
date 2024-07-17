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

package com.alibaba.polardbx.manager.handler;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.transaction.TransactionManager;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

public class LockTransHandler {

    /**
     * Schema -> {stamped lock, lock date}
     */
    private static final Map<String, SchemaLock> lockedSchemas = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private static final Logger logger = LoggerFactory.getLogger(LockTransHandler.class);

    private static final int FIELD_COUNT = 4;

    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("STATE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DURATION", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void lock(String sql, ManagerConnection c) {
        String schema = extractSchema(sql);
        checkSchema(schema);

        ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();
        if (!(tm instanceof TransactionManager)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "LOCK TRANS is only available when distributed transaction is enabled");
        }

        synchronized (lockedSchemas) {
            if (lockedSchemas.putIfAbsent(schema, new SchemaLock()) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Schema " + schema + " is already locked");
            }
        }

        logger.info("Locking distributed transaction of schema " + schema);
        SchemaLock schemaLock = lockedSchemas.get(schema);
        schemaLock.date = new Date();
        schemaLock.lock = ((TransactionManager) tm).getLock();
        try {
            schemaLock.stamp = schemaLock.lock.writeLock();
        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }

        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
    }

    public static void unlock(String sql, ManagerConnection c) {
        String schema = extractSchema(sql);
        checkSchema(schema);
        ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();
        if (!(tm instanceof TransactionManager)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS,
                "UNLOCK TRANS is only available when distributed transaction is enabled");
        }

        SchemaLock schemaLock;
        synchronized (lockedSchemas) {
            schemaLock = lockedSchemas.remove(schema);
            if (null == schemaLock) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS, "Schema " + schema + " is not locked");
            }
        }

        try {
            schemaLock.lock.unlock(schemaLock.stamp);
        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }

        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
    }

    public static void show(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        byte packetId = eof.packetId;

        synchronized (lockedSchemas) {
            for (Map.Entry<String, SchemaLock> entry : lockedSchemas.entrySet()) {
                String schema = entry.getKey();
                long lockTime = entry.getValue().date.getTime();
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(schema, c.getResultSetCharset())); // schema
                row.add(StringUtil.encode("LOCKED", c.getResultSetCharset())); // state
                row.add(StringUtil.encode(new Timestamp(lockTime).toString(), c.getResultSetCharset())); // start_time
                row.add(LongUtil.toBytes((System.currentTimeMillis() - lockTime) / 1000)); // duration
                row.packetId = ++packetId;
                proxy = row.write(proxy);
            }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static String extractSchema(String command) {
        String[] splits = command.split("\\s+");
        if (splits.length == 3) {
            return splits[2];
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER, "Syntax Error: Schema not specified");
        }
    }

    private static void checkSchema(String schema) {
        if (schema == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NO_DB_ERROR);
        }

        SchemaConfig schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get(schema);
        if (schemaConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schema);
        }

        TDataSource ds = schemaConfig.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    private static class SchemaLock {
        public StampedLock lock;
        public long stamp;
        public Date date;
    }
}
