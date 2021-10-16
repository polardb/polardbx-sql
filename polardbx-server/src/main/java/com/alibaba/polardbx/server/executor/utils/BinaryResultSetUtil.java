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

import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.BinaryResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.BinaryRowDataMultiPacket;
import com.alibaba.polardbx.net.packet.BinaryRowDataPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MysqlBinaryResultSetPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxy;
import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.Field;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.matrix.jdbc.TResultSetMetaData;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.io.UnsupportedEncodingException;
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

    public static IPacketOutputProxy resultSetToPacket(ResultSet rs, String charset, ServerConnection c,
                                                       AtomicLong affectRow) throws SQLException,
        IllegalAccessException,
        UnsupportedEncodingException {

        MysqlBinaryResultSetPacket packet = new MysqlBinaryResultSetPacket();
        // 先执行一次next，因为存在lazy-init处理，可能写了packet head包出去，但实际获取数据时出错导致客户端出现lost
        // connection，没有任何其他异常
        boolean existNext = rs.next();

        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();
        boolean existUndecidedType = false;
        Set<Integer> undecidedTypeIndexs = new HashSet<Integer>();

        synchronized (packet) {
            if (packet.resulthead == null) {
                packet.resulthead = new BinaryResultSetHeaderPacket();
                packet.resulthead.column_count = colunmCount;
            }

            int charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(charset, null);
            // int charsetIndex =
            // CharsetMapping.getCharsetIndexForMysqlEncodingName(charset);
            String javaCharset = CharsetUtil.getJavaCharset(charset);

            if (colunmCount > 0) {
                if (packet.fieldPackets == null) {
                    packet.fieldPackets = new FieldPacket[colunmCount];
                    for (int i = 0; i < colunmCount; i++) {
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
                            packet.fieldPackets[i].catalog = StringUtil.encode_0("def", javaCharset);
                            packet.fieldPackets[i].orgName =
                                StringUtil.encode_0(((TResultSetMetaData) metaData).getOriginColumnName(j),
                                    javaCharset);
                            packet.fieldPackets[i].orgName =
                                StringUtil.encode_0(metaData.getColumnName(j), javaCharset);
                            packet.fieldPackets[i].name = StringUtil.encode_0(metaData.getColumnLabel(j), javaCharset);
                            packet.fieldPackets[i].orgTable =
                                StringUtil.encode_0(((TResultSetMetaData) metaData).getOriginTableName(j),
                                    javaCharset);
                            packet.fieldPackets[i].table = StringUtil.encode_0(metaData.getTableName(j), javaCharset);
                            // 使用逻辑表名
                            packet.fieldPackets[i].db = StringUtil.encode_0(c.getSchema(), javaCharset);
                            packet.fieldPackets[i].length = metaData.getColumnDisplaySize(j);
                            packet.fieldPackets[i].flags = ResultSetUtil.toFlag(metaData, j, meta);
                            packet.fieldPackets[i].flags |= 128;
                            packet.fieldPackets[i].decimals = (byte) metaData.getScale(j);
                            packet.fieldPackets[i].charsetIndex = charsetIndex;
                            if (DataTypeUtil.anyMatchSemantically(meta.getDataType(), DataTypes.BinaryType,
                                DataTypes.BinaryStringType, DataTypes.BlobType)) {
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
                                undecidedTypeIndexs.add(i);
                                existUndecidedType = true;
                            }
                        } else {
                            throw new NotSupportException();
                        }
                    }
                }
            }
        }

        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // 如果没有出现未决类型，直接输入header
        if (!existUndecidedType) {
            proxy = packet.write(proxy);
        }

        List<BinaryRowDataPacket> lazyRaws = new ArrayList<BinaryRowDataPacket>();
        if (existNext) {
            do {
                BinaryRowDataPacket row = new BinaryRowDataMultiPacket(colunmCount);
                for (int i = 0; i < colunmCount; i++) {
                    int j = i + 1;
                    if (existUndecidedType && undecidedTypeIndexs.contains(i)) {
                        // 根据数据的类型，重新设置下type
                        DataType type = DataTypes.StringType;
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
                        undecidedTypeIndexs.remove(Integer.valueOf(i)); // 必须是对象
                        packet.fieldPackets[i].type =
                            (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(type.getSqlType(),
                                packet.fieldPackets[i].decimals)) & 0xff);
                    }

                    byte[] bytes = MysqlDefs.resultSetToByte(rs,
                        j,
                        MysqlDefs.MySQLTypeUInt(packet.fieldPackets[i].type),
                        ResultSetUtil.isUnsigned(packet.fieldPackets[i].flags),
                        charset);
                    if (packet.fieldPackets[i].type == MysqlDefs.FIELD_TYPE_BIT) {
                        row.fieldValues.add(bytes);
                    } else if (packet.fieldPackets[i].type == MysqlDefs.FIELD_TYPE_DECIMAL) {
                        // MutableInt scale = new MutableInt();
                        row.fieldValues.add(bytes);
                    } else {
                        row.fieldValues.add(bytes == null ? null : bytes);
                    }
                }

                // 如果出现未决类型，而且结果集有数据，则先输出header
                if (existUndecidedType && undecidedTypeIndexs.size() == 0) {
                    proxy = packet.write(proxy);
                    existUndecidedType = false;
                }

                // 如果这里已经没有未决类型, 输出每一行的数据，
                // 否则，先放在lazyRow中缓存
//                row.packetId = c.getNewPacketId();
                if (!existUndecidedType || undecidedTypeIndexs.size() == 0) {
                    // flush before lazyRows
                    for (BinaryRowDataPacket lazyRow : lazyRaws) {
                        proxy = lazyRow.write(proxy);
                    }
                    lazyRaws.clear();

                    proxy = row.write(proxy);
                } else {
                    lazyRaws.add(row);
                }

                affectRow.incrementAndGet();
            } while (rs.next());
        }

        // if it's still undecided, output header and rows finally
        // 如果这里还是出现未决类型，说明结果集是没有数据，则强制输出packet的header 和 laszRow数据
        if (existUndecidedType) {
            proxy = packet.write(proxy);
            for (BinaryRowDataPacket row : lazyRaws) {
                proxy = row.write(proxy);
            }

            lazyRaws.clear();
        }

        return proxy;
    }
}
