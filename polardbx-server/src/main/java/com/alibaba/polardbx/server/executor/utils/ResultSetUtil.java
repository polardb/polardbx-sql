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

package com.alibaba.polardbx.server.executor.utils;

import com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxy;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.matrix.jdbc.TResultSetMetaData;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MysqlResultSetPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataMultiPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.util.StringUtil;
import com.mysql.jdbc.Field;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 将resultset转为二进制包
 *
 * @author mengshi.sunmengshi 2013-11-19 下午4:27:55
 * @since 3.0.1
 */
public class ResultSetUtil {
    public final static long NO_SQL_SELECT_LIMIT = -88;
    public final static long MAX_SQL_SELECT_LIMIT = 1000_000_000_000_000L;

    public static int toFlag(TResultSetMetaData metaData, int column, ColumnMeta meta) throws SQLException {
        int flags = toFlag(metaData, column);
        DataType dataType = meta.getDataType();
        if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BinaryType)) {
            flags |= 128;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BlobType)) {
            flags |= 128;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BinaryStringType)) {
            flags |= 128;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BytesType)) {
            // is binary
            flags |= 128;

            // is blob
            flags |= 16;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.VarcharType)
            && dataType.getCharsetName() == CharsetName.BINARY) {
            flags |= 128;
        }

        if (meta.getField().isPrimary()) {
            flags |= 2;
        }

        return flags;
    }

    /**
     * <pre>
     *
     * Flags(两个字节，共16个BIT)的各个Bit所用于标记列的相关定义描述
     *
     *  1 (第1个Bit):
     *      isNotNull: ((this.colFlag & 1) > 0)
     *  2 (第2个Bit):
     *      isPrimaryKey: ((this.colFlag & 2) > 0)
     *  4 (第3个Bit)：
     *      isUniqueKey: ((this.colFlag & 4) > 0)
     *  8 (第4个Bit)：
     *      isMultipleKey: ((this.colFlag & 8) > 0)
     *  16 (第5个Bit)：
     *      isBlob: ((this.colFlag & 16) > 0);
     *  32 (第6个Bit)：
     *      isUnsigned: ((this.colFlag & 32) > 0)
     *  64 (第7个Bit)：
     *      isZeroFill: ((this.colFlag & 64) > 0)
     *  128 (第8个Bit):
     *      isBinary:  ((this.colFlag & 128) > 0)
     * </pre>
     */
    public static int toFlag(TResultSetMetaData metaData, int column) throws SQLException {
        int flags = 0;
        if (metaData.isNullable(column) != ResultSetMetaData.columnNullable) {
            flags |= 1;
        }
        com.alibaba.polardbx.optimizer.config.table.Field field = metaData.getField(column);
        if (field.isPrimary()) {
            flags |= 1;
            flags |= 2;
        }

        if (!metaData.isSigned(column)) {
            flags |= 32;
        }

        if (metaData.isAutoIncrement(column)) {
            flags |= 512;
        }

        return flags;
    }

    public static boolean isUnsigned(int flag) {
        return (flag & 32) > 0;
    }

    public static boolean isBinary(int charsetIndex) {
        return charsetIndex == 63;
    }

    // ResultSetMetaData.fields
    static java.lang.reflect.Field fieldsField = null;
    static java.lang.reflect.Field bufferField = null;

    static {
        try {
            fieldsField = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            fieldsField.setAccessible(true);

            bufferField = Field.class.getDeclaredField("buffer");
            bufferField.setAccessible(true);
        } catch (SecurityException e) {
            throw GeneralUtil.nestedException(e);
        } catch (NoSuchFieldException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static IPacketOutputProxy resultSetToPacket(ResultSet rs, String charset, FrontendConnection c,
                                                       AtomicLong affectRow, IPacketOutputProxy proxy,
                                                       long sqlSelectLimit)
        throws Exception {

        boolean withProxy = proxy != null;
        if (withProxy) {
            proxy.packetBegin();
        }
        MysqlResultSetPacket packet = new MysqlResultSetPacket();
        // 先执行一次next，因为存在lazy-init处理，可能写了packet head包出去，但实际获取数据时出错导致客户端出现lost
        // connection，没有任何其他异常
        boolean existNext = rs.next();

        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        boolean existUndecidedType = false;
        final Set<Integer> undecidedTypeIndexes = new HashSet<>();
        synchronized (packet) {
            if (packet.resultHead == null) {
                packet.resultHead = new ResultSetHeaderPacket();
                packet.resultHead.fieldCount = columnCount;
            }
            String javaCharset = CharsetUtil.getJavaCharset(charset);
            int charsetIndex = CharsetUtil.getIndex(charset);
            if (columnCount > 0) {
                if (packet.fieldPackets == null) {
                    while (metaData instanceof ResultSetMetaDataProxy) {
                        java.sql.ResultSetMetaData newMetaData =
                            ((ResultSetMetaDataProxy) metaData).getResultSetMetaDataRaw();
                        if (newMetaData == metaData) {
                            break;
                        }
                        metaData = newMetaData;
                    }
                    packet.fieldPackets = new FieldPacket[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        int j = i + 1;
                        packet.fieldPackets[i] = new FieldPacket();
                        if (metaData instanceof com.mysql.jdbc.ResultSetMetaData) {
                            Field[] fields = (Field[]) fieldsField.get(metaData);
                            packet.fieldPackets[i] = new FieldPacket();
                            packet.fieldPackets[i].unpacked = (byte[]) bufferField.get(fields[i]);
                        } else if (metaData instanceof TResultSetMetaData) {
                            List<ColumnMeta> metas = ((TResultSetMetaData) metaData).getColumnMetas();
                            ColumnMeta meta = metas.get(i);
                            packet.fieldPackets[i].catalog =
                                StringUtil.encode_0(FieldPacket.DEFAULT_CATALOG_STR, javaCharset);
                            packet.fieldPackets[i].orgName =
                                StringUtil.encode_0(((TResultSetMetaData) metaData).getOriginColumnName(j),
                                    javaCharset);
                            packet.fieldPackets[i].name = StringUtil.encode_0(metaData.getColumnLabel(j), javaCharset);
                            packet.fieldPackets[i].orgTable =
                                StringUtil.encode_0(((TResultSetMetaData) metaData).getOriginTableName(j),
                                    javaCharset);
                            packet.fieldPackets[i].table = StringUtil.encode_0(metaData.getTableName(j), javaCharset);
                            packet.fieldPackets[i].db = StringUtil.encode_0(c.getSchema(), javaCharset);
                            packet.fieldPackets[i].length = metaData.getColumnDisplaySize(j);
                            packet.fieldPackets[i].flags = toFlag((TResultSetMetaData) metaData, j, meta);
                            packet.fieldPackets[i].decimals = (byte) metaData.getScale(j);
                            final com.alibaba.polardbx.optimizer.config.table.Field field = meta.getField();
                            final Integer collationIndex = field.getCollationIndex();
                            if (collationIndex != null && collationIndex > 0) {
                                packet.fieldPackets[i].charsetIndex = collationIndex;
                            } else {
                                if (DataTypeUtil.anyMatchSemantically(meta.getDataType(), DataTypes.BinaryType,
                                    DataTypes.BlobType)) {
                                    packet.fieldPackets[i].charsetIndex = 63; // iso-8859-1
                                } else if (DataTypeUtil.equalsSemantically(meta.getDataType(), DataTypes.VarcharType)
                                    && meta.getDataType().getCharsetName() == CharsetName.BINARY) {
                                    packet.fieldPackets[i].charsetIndex = 63; // iso-8859-1
                                } else {
                                    packet.fieldPackets[i].charsetIndex = charsetIndex;
                                }
                            }

                            int sqlType = ((TResultSetMetaData) metaData).getColumnType(j, true);
                            if (sqlType != DataType.UNDECIDED_SQL_TYPE) {
                                final int i1 = MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(sqlType,
                                    packet.fieldPackets[i].decimals)) & 0xff;
                                /**
                                 * In Jdbc8,the blob type with collation
                                 * utf8_general_ci (33) together will be
                                 * converted to text more to see:
                                 * NativeProtocol.findMysqlType but it is not
                                 * correct in columns of system tables; so we
                                 * need deal it earlier.
                                 */
                                // if (i1 >= 249 && i1 <= 252) {// blob
                                // packet.fieldPackets[i].charsetIndex = 63; //
                                // iso-8859-1
                                // }
                                packet.fieldPackets[i].type = (byte) i1;
                            } else {
                                packet.fieldPackets[i].type = (byte) (MysqlDefs.FIELD_TYPE_NULL & 0xff); // 默认设置为string
                                undecidedTypeIndexes.add(i);
                                existUndecidedType = true;
                            }
                        } else {
                            throw new NotSupportException();
                        }
                    }
                }
            }
        }

        // 如果未出现未决类型，先输出header
        // 如果出现未决类型，但没有数据，强行输出header
        // 如果出现未决类型，并且有数据, 等拿到第一条数据后再输出
        if (!existUndecidedType || !existNext) {
            proxy = withProxy ? writeHeaderWithProxy(packet, c, proxy) : writeHeader(packet, c);
            existUndecidedType = false;

        }

        do {
            if (!existNext) {
                // 不存在记录，直接退出
                break;
            }
            if (sqlSelectLimit != NO_SQL_SELECT_LIMIT && sqlSelectLimit < MAX_SQL_SELECT_LIMIT
                && sqlSelectLimit-- <= 0L) {
                break;
            }
            RowDataPacket row = null;
            row = new RowDataMultiPacket(columnCount, c::getNewPacketId);
            final XRowSet xRowSet =
                (rs instanceof TResultSet && ((TResultSet) rs).getCurrentKVPair() instanceof XRowSet) ?
                    (XRowSet) ((TResultSet) rs).getCurrentKVPair() : null;
            for (int i = 0; i < columnCount; i++) {
                int j = i + 1;
                if (existUndecidedType && undecidedTypeIndexes.contains(i)) {
                    resetUndecidedType(rs, i, packet, undecidedTypeIndexes);
                }

                if (xRowSet != null) {
                    // Fast path of X-Protocol.
                    row.fieldValues.add(xRowSet.fastGetBytes(i, charset));
                } else if (rs instanceof TResultSet) {
                    row.fieldValues.add(((TResultSet) rs).getBytes(j, charset));
                } else {
                    row.fieldValues.add(rs.getBytes(j));
                }
            }

            if (existUndecidedType) {// 如果出现未决类型，一条数据都没有，强制输出packet
                proxy = withProxy ? writeHeaderWithProxy(packet, c, proxy) : writeHeader(packet, c);
                existUndecidedType = false;
            }

            proxy = row.write(proxy);
            affectRow.incrementAndGet();// 计数
            existNext = rs.next();
        } while (existNext);

        if (existUndecidedType) {// 如果出现未决类型， 一条数据都没有，强制输出packet
            proxy = withProxy ? writeHeaderWithProxy(packet, c, proxy) : writeHeader(packet, c);
        }

        if (withProxy) {
            writeEOFPacket(proxy, c, EOFPacket.SERVER_MORE_RESULTS_EXISTS);
            proxy.packetEnd();
        }
        return proxy;
    }

    public static void eofToPacket(IPacketOutputProxy proxy, FrontendConnection c, int statusFlags) {
        // packetBegin在ResultSetUtil中
        // // write last eof
        writeEOFPacket(proxy, c, statusFlags);

        // write buffer
        proxy.packetEnd();
    }

    private static void writeEOFPacket(IPacketOutputProxy proxy, FrontendConnection c, int status) {
        if (c.isEofDeprecated()) {
            OkPacket ok = new OkPacket(true);
            ok.packetId = c.getNewPacketId();
            ok.serverStatus = status;
            ok.write(proxy);
            return;
        }
        proxy.packetBegin();

        proxy.checkWriteCapacity(c.getPacketHeaderSize() + EOFPacket.PACKET_LEN);
        proxy.writeUB3(EOFPacket.PACKET_LEN);
        proxy.write(c.getNewPacketId());

        proxy.write(EOFPacket.EOF_HEADER);
        proxy.writeUB2(0);
        proxy.writeUB2(status);

        proxy.packetEnd();
    }

    public static IPacketOutputProxy writeHeader(MysqlResultSetPacket packet, FrontendConnection c) {
        // write EOF packet for header by client flag
        return writeHeader(packet, c, !c.isEofDeprecated());
    }

    public static IPacketOutputProxy writeHeader(MysqlResultSetPacket packet, FrontendConnection c, boolean writeEof) {
        // write header
        packet.resultHead.packetId = c.getNewPacketId();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c);
        proxy.packetBegin();

        proxy = packet.resultHead.write(proxy);

        // write fields
        if (packet.fieldPackets != null) {
            for (FieldPacket field : packet.fieldPackets) {
                field.packetId = c.getNewPacketId();
                proxy = field.write(proxy);
            }
        }

        // write eof
        if (writeEof) {
            writeEOFPacket(proxy, c, EOFPacket.SERVER_STATUS_AUTOCOMMIT);
        }

        return proxy;
    }

    private static IPacketOutputProxy writeHeaderWithProxy(MysqlResultSetPacket packet, FrontendConnection c,
                                                           IPacketOutputProxy proxy) {
        // write header
        packet.resultHead.packetId = c.getNewPacketId();

        proxy = packet.resultHead.write(proxy);

        // write fields
        if (packet.fieldPackets != null) {
            for (FieldPacket field : packet.fieldPackets) {
                field.packetId = c.getNewPacketId();
                proxy = field.write(proxy);
            }
        }

        // write eof
        if (!c.isEofDeprecated()) {
            writeEOFPacket(proxy, c, EOFPacket.SERVER_STATUS_AUTOCOMMIT);
        }
        return proxy;
    }

    /**
     * Reset data-type of the (i+1)-th column in the result set,
     * i.e., the i-th element of headerPacket.fieldPackets[] array.
     */
    public static void resetUndecidedType(ResultSet rs, int i,
                                          MysqlResultSetPacket headerPacket,
                                          Set<Integer> undecidedTypeIndexes) {
        // 根据数据的类型，重新设置下type
        DataType type = DataTypes.StringType;
        // Result set begins with 1, but array in java begins with 0.
        final int j = i + 1;
        try {
            Object obj = rs.getObject(j);

            if (obj != null) {
                DataType objType = DataTypeUtil.getTypeOfObject(obj);// 将JavaTypeObject转换为Tddl的DataType
                if (objType.getSqlType() != DataType.UNDECIDED_SQL_TYPE) {
                    type = objType;
                }
            }

        } catch (Throwable e) {
            // ignore
            // 针对0000-00-00的时间类型可能getObject会失败，getBytes没问题
        }
        undecidedTypeIndexes.remove(Integer.valueOf(i)); // 必须是对象
        headerPacket.fieldPackets[i].type =
            (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(type.getSqlType(),
                headerPacket.fieldPackets[i].decimals)) & 0xff);
    }

}
