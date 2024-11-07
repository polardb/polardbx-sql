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
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.matrix.jdbc.TResultSetMetaData;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.BinaryResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.BinaryRowDataMultiPacket;
import com.alibaba.polardbx.net.packet.BinaryRowDataPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MysqlBinaryResultSetPacket;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.conn.ResultSetCachedObj;
import com.alibaba.polardbx.server.util.StringUtil;
import com.mysql.jdbc.Field;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by simiao.zw on 2014/8/4. Use by COM_STMT_EXECUTE return response
 * http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
 */
public class BinaryResultSetUtil {

    /**
     * Return both header packet and data packet.
     */
    public static IPacketOutputProxy resultSetToPacket(ResultSet rs, String charset, ServerConnection c,
                                                       AtomicLong affectRow, PreparedStmtCache preparedStmtCache,
                                                       long sqlSelectLimit)
        throws SQLException, IllegalAccessException {


        // 先执行一次next，因为存在lazy-init处理，可能写了packet head包出去，但实际获取数据时出错导致客户端出现lost
        // connection，没有任何其他异常
        boolean existNext = rs.next();

        final Set<Integer> undecidedTypeIndexes = new HashSet<>();
        final MysqlBinaryResultSetPacket headerPacket = new MysqlBinaryResultSetPacket();

        // Process header packet.
        processHeader(rs, c, headerPacket, undecidedTypeIndexes, preparedStmtCache);

        // Process data packet.
        return processData(rs, c, charset, affectRow, headerPacket, undecidedTypeIndexes, existNext, sqlSelectLimit);
    }

    /**
     * Only return header packet (without data).
     */
    public static IPacketOutputProxy resultSetToHeaderPacket(ResultSetCachedObj resultSetCachedObj,
                                                             ServerConnection c, PreparedStmtCache preparedStmtCache,
                                                             AtomicLong affectRows)
        throws SQLException, IllegalAccessException {
        // Call the resultSet.next() to actually execute the physical sql
        // and generate data. If the result set is empty, set the last row flag.
        final ResultSet rs = resultSetCachedObj.getResultSet();
        resultSetCachedObj.setLastRow(!rs.next());
        resultSetCachedObj.setFirstRow(true);
        affectRows.set(resultSetCachedObj.getRowCount());

        final MysqlBinaryResultSetPacket packet = new MysqlBinaryResultSetPacket();
        final Set<Integer> undecidedTypeIndexes = new HashSet<>();

        processHeader(rs, c, packet, undecidedTypeIndexes, preparedStmtCache);

        // Do not write EOF for this header,
        // since an EOF packet will be written in
        // ServerConnection.ServerResultHandler.sendPacketEnd().
        return ResultSetUtil.writeHeader(packet, c, false);
    }

    /**
     * Only return data packet (without header).
     */
    public static IPacketOutputProxy resultSetToDataPacket(ResultSetCachedObj resultSetCachedObj,
                                                           String charset,
                                                           ServerConnection c,
                                                           int fetchRows,
                                                           PreparedStmtCache preparedStmtCache)
        throws SQLException, IllegalAccessException {
        final ResultSet rs = resultSetCachedObj.getResultSet();

        // Although we do not write header into output proxy,
        // we need some meta-data to get the correct data type.
        final MysqlBinaryResultSetPacket headerPacket = new MysqlBinaryResultSetPacket();
        final Set<Integer> undecidedTypeIndexes = new HashSet<>();
        processHeader(rs, c, headerPacket, undecidedTypeIndexes, preparedStmtCache);

        boolean existUndecidedType = !undecidedTypeIndexes.isEmpty();

        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        List<BinaryRowDataPacket> lazyRows = new ArrayList<>();
        while (fetchRows > 0) {
            fetchRows--;
            // For the first row, the rs.next() is already called when the header packet is sent.
            // So do not call it again or we will miss the first row of data.
            if (!resultSetCachedObj.isFirstRow()) {
                if (!resultSetCachedObj.isLastRow()) {
                    resultSetCachedObj.setLastRow(!rs.next());
                }
            } else {
                resultSetCachedObj.setFirstRow(false);
            }

            if (resultSetCachedObj.isLastRow()) {
                resultSetCachedObj.close();
                break;
            }

            BinaryRowDataPacket row =
                getRowDataPacket(rs, charset, headerPacket, undecidedTypeIndexes, existUndecidedType);

            if (existUndecidedType && undecidedTypeIndexes.size() == 0) {
                existUndecidedType = false;
            }

            // 如果这里已经没有未决类型, 输出每一行的数据，
            // 否则，先放在lazyRow中缓存
            if (!existUndecidedType) {
                // flush before lazyRows
                for (BinaryRowDataPacket lazyRow : lazyRows) {
                    proxy = lazyRow.write(proxy);
                }
                lazyRows.clear();

                proxy = row.write(proxy);
            } else {
                lazyRows.add(row);
            }
        }

        // if it's still undecided, output header and rows finally
        // 如果这里还是出现未决类型，说明结果集是没有数据，则强制输出 lazyRow 数据
        if (existUndecidedType) {
            for (BinaryRowDataPacket row : lazyRows) {
                proxy = row.write(proxy);
            }

            lazyRows.clear();
        }

        return proxy;
    }

    /**
     * Add result set header (meta-data) into packet.
     */
    private static void processHeader(ResultSet rs, ServerConnection c, MysqlBinaryResultSetPacket packet,
                                      Set<Integer> undecidedTypeIndexes, PreparedStmtCache preparedStmtCache)
        throws SQLException, IllegalAccessException {
        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        synchronized (packet) {
            if (packet.resultHead == null) {
                packet.resultHead = new BinaryResultSetHeaderPacket();
                packet.resultHead.fieldCount = columnCount;
            }

            int charsetIndex = preparedStmtCache.getCharsetIndex();
            String javaCharset = preparedStmtCache.getJavaCharset();

            if (columnCount > 0 && packet.fieldPackets == null) {
                if (preparedStmtCache.getFieldPackets() != null) {
                    // Use fieldPackets in preparedStmtCache.
                    packet.fieldPackets = preparedStmtCache.getFieldPackets();
                    return;
                }
                // Generate fieldPackets since preparedStmtCache has no fieldPackets.
                packet.fieldPackets = new FieldPacket[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    int j = i + 1;
                    packet.fieldPackets[i] = new FieldPacket();
                    while (metaData instanceof ResultSetMetaDataProxy) {
                        java.sql.ResultSetMetaData newMetaData =
                            ((ResultSetMetaDataProxy) metaData).getResultSetMetaDataRaw();
                        if (newMetaData == metaData) {
                            break;
                        }
                        metaData = newMetaData;
                    }

                    if (metaData instanceof com.mysql.jdbc.ResultSetMetaData) {
                        Field[] fields = (Field[]) ResultSetUtil.fieldsField.get(metaData);
                        packet.fieldPackets[i] = new FieldPacket();
                        packet.fieldPackets[i].unpacked = (byte[]) ResultSetUtil.bufferField.get(fields[i]);
                        // since later for row need column type & scale,
                        // so copy it here.
                        packet.fieldPackets[i].type = fields[i].getMysqlType();
                        packet.fieldPackets[i].decimals = (byte) fields[i].getPrecisionAdjustFactor();
                    } else if (metaData instanceof TResultSetMetaData) {
                        List<ColumnMeta> metas = ((TResultSetMetaData) metaData).getColumnMetas();
                        ColumnMeta meta = metas.get(i);
                        packet.fieldPackets[i].catalog =
                            StringUtil.encode_0(FieldPacket.DEFAULT_CATALOG_STR, javaCharset);
                        packet.fieldPackets[i].orgName =
                            StringUtil.encode_0(((TResultSetMetaData) metaData).getOriginColumnName(j),
                                javaCharset);
                        packet.fieldPackets[i].name =
                            StringUtil.encode_0(metaData.getColumnLabel(j), javaCharset);
                        packet.fieldPackets[i].orgTable =
                            StringUtil.encode_0(((TResultSetMetaData) metaData).getOriginTableName(j),
                                javaCharset);
                        packet.fieldPackets[i].table =
                            StringUtil.encode_0(metaData.getTableName(j), javaCharset);
                        // 使用逻辑表名
                        packet.fieldPackets[i].db = StringUtil.encode_0(c.getSchema(), javaCharset);
                        packet.fieldPackets[i].length = metaData.getColumnDisplaySize(j);
                        packet.fieldPackets[i].flags = ResultSetUtil.toFlag((TResultSetMetaData) metaData, j, meta);
                        packet.fieldPackets[i].flags |= 128;
                        packet.fieldPackets[i].decimals = (byte) metaData.getScale(j);
                        packet.fieldPackets[i].charsetIndex = charsetIndex;
                        if (DataTypeUtil.anyMatchSemantically(meta.getDataType(), DataTypes.BinaryType,
                            DataTypes.BinaryStringType, DataTypes.BlobType)) {
                            packet.fieldPackets[i].charsetIndex = 63; // iso-8859-1
                        } else if (DataTypeUtil.equalsSemantically(meta.getDataType(), DataTypes.VarcharType)
                            && meta.getDataType().getCharsetName() == CharsetName.BINARY) {
                            packet.fieldPackets[i].charsetIndex = 63; // iso-8859-1
                        } else {
                            packet.fieldPackets[i].charsetIndex = charsetIndex;
                        }

                        int sqlType = ((TResultSetMetaData) metaData).getColumnType(j, true);
                        if (sqlType != DataType.UNDECIDED_SQL_TYPE) {
                            packet.fieldPackets[i].type =
                                (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(sqlType,
                                    packet.fieldPackets[i].decimals)) & 0xff);

                        } else {
                            packet.fieldPackets[i].type = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
                            undecidedTypeIndexes.add(i);
                        }
                    } else {
                        throw new NotSupportException();
                    }
                    preparedStmtCache.setFieldPackets(packet.fieldPackets);
                }
            }
        }
    }

    /**
     * We assume the headerPacket already contains the header,
     * and we first write the header packet into the output proxy,
     * and then put all rows of data into the output proxy.
     */
    private static IPacketOutputProxy processData(ResultSet rs, ServerConnection c, String charset,
                                                  AtomicLong affectRow, MysqlBinaryResultSetPacket headerPacket,
                                                  Set<Integer> undecidedTypeIndexes, boolean existNext,
                                                  long sqlSelectLimit)
        throws SQLException {
        boolean existUndecidedType = !undecidedTypeIndexes.isEmpty();

        IPacketOutputProxy proxy = null;
        // 如果没有出现未决类型，直接输入header
        if (!existUndecidedType) {
            proxy = ResultSetUtil.writeHeader(headerPacket, c);
        }

        List<BinaryRowDataPacket> lazyRows = new ArrayList<>();
        if (existNext) {
            do {
                if (sqlSelectLimit != ResultSetUtil.NO_SQL_SELECT_LIMIT
                    && sqlSelectLimit < ResultSetUtil.MAX_SQL_SELECT_LIMIT && sqlSelectLimit-- <= 0L) {
                    break;
                }
                final BinaryRowDataPacket row =
                    getRowDataPacket(rs, charset, headerPacket, undecidedTypeIndexes, existUndecidedType);

                // 如果出现未决类型，而且结果集有数据，则先输出header
                if (existUndecidedType && undecidedTypeIndexes.size() == 0) {
                    proxy = ResultSetUtil.writeHeader(headerPacket, c);
                    existUndecidedType = false;
                }

                // 如果这里已经没有未决类型, 输出每一行的数据，
                // 否则，先放在lazyRow中缓存
                if (!existUndecidedType || undecidedTypeIndexes.size() == 0) {
                    // flush before lazyRows
                    for (BinaryRowDataPacket lazyRow : lazyRows) {
                        proxy = lazyRow.write(proxy);
                    }
                    lazyRows.clear();

                    proxy = row.write(proxy);
                } else {
                    lazyRows.add(row);
                }

                affectRow.incrementAndGet();
            } while (rs.next());
        }

        // if it's still undecided, output header and rows finally
        // 如果这里还是出现未决类型，说明结果集是没有数据，则强制输出packet的header 和 lazyRow数据
        if (existUndecidedType) {
            proxy = ResultSetUtil.writeHeader(headerPacket, c);
            for (BinaryRowDataPacket row : lazyRows) {
                proxy = row.write(proxy);
            }

            lazyRows.clear();
        }

        return proxy;
    }

    /**
     * Get a single row of data from the result set.
     */
    private static BinaryRowDataPacket getRowDataPacket(ResultSet rs, String charset,
                                                        MysqlBinaryResultSetPacket headerPacket,
                                                        Set<Integer> undecidedTypeIndexes,
                                                        boolean existUndecidedType) throws SQLException {
        final int columnCount = headerPacket.resultHead.fieldCount;
        final BinaryRowDataPacket row = new BinaryRowDataMultiPacket(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int j = i + 1;
            if (existUndecidedType && undecidedTypeIndexes.contains(i)) {
                ResultSetUtil.resetUndecidedType(rs, i, headerPacket, undecidedTypeIndexes);
            }

            byte[] bytes = MysqlDefs.resultSetToByte(
                rs,
                j,
                MysqlDefs.MySQLTypeUInt(headerPacket.fieldPackets[i].type),
                ResultSetUtil.isUnsigned(headerPacket.fieldPackets[i].flags),
                ResultSetUtil.isBinary(headerPacket.fieldPackets[i].charsetIndex),
                charset);

            row.fieldValues.add(bytes);
        }
        return row;
    }
}
