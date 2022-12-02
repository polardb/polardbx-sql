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

package com.alibaba.polardbx.executor.partitionvisualizer;

import java.util.Map;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.model.PartitionHeatInfo;

import io.airlift.slice.Slice;

/**
 * @author ximing.yd
 * @date 2022/4/14 3:29 下午
 */
public class VisualConvertUtil {

    private static final Logger logger = LoggerFactory.getLogger(VisualConvertUtil.class);

    public static String generateBound(PartitionHeatInfo pInfo){
        if (pInfo == null) {
            logger.warn("pInfo is null");
            return "-,-,-,-";
        }
        return String.format("%s,%s,%s,%s", pInfo.getSchemaName(), pInfo.getLogicalTable(),
            fillZero(pInfo.getPartitionSeq()), pInfo.getPartitionName());
    }

    //为了在bound按照字母序排序时不会出现错乱
    public static String fillZero(Integer originNum){
        if (originNum == null) {
            logger.warn("originNum is null");
            return "-";
        }
        if (originNum < 10) {
            return String.format("000%s", originNum);
        }
        if (originNum < 100) {
            return String.format("00%s", originNum);
        }
        if (originNum < 1000) {
            return String.format("0%s", originNum);
        }
        return originNum.toString();
    }


    public static String generatePartitionHeatInfoKey(PartitionHeatInfo pInfo) {
        return String.format("%s,%s,%s", pInfo.getSchemaName(), pInfo.getLogicalTable(), pInfo.getPartitionName());
    }


    public static String getObjString(String key, Map<String, Object> objMap){
        if (objMap.get(key) instanceof Slice) {
            return ((Slice)objMap.get(key)).toStringUtf8();
        }
        if (objMap.get(key) instanceof String) {
            return ((String)objMap.get(key));
        }
        return "";
    }

    public static Integer getObjInteger(String key, Map<String, Object> objMap){
        if (objMap.get(key) instanceof UInt64) {
            return ((UInt64)objMap.get(key)).intValue();
        }
        if (objMap.get(key) instanceof Number) {
            return ((Number)objMap.get(key)).intValue();
        }
        if (objMap.get(key) instanceof Integer) {
            return ((Integer)objMap.get(key));
        }
        return 0;
    }

    public static Long getObjLong(String key, Map<String, Object> objMap){
        if (objMap.get(key) instanceof UInt64) {
            return ((UInt64)objMap.get(key)).longValue();
        }
        if (objMap.get(key) instanceof Number) {
            return ((Number)objMap.get(key)).longValue();
        }
        if (objMap.get(key) instanceof Long) {
            return ((Long)objMap.get(key));
        }
        return 0L;
    }


    public static Long getObjLong(Object obj) {
        if (obj instanceof UInt64) {
            return ((UInt64)obj).longValue();
        }
        if (obj instanceof Number) {
            return ((Number)obj).longValue();
        }
        if (obj instanceof Long) {
            return ((Long)obj);
        }
        return 0L;
    }

    public static String genStoreIdTableKey(PartitionHeatInfo info) {
        return String.format("%s,%s,%s", info.getStorageInstId(), info.getSchemaName(), info.getLogicalTable());
    }

    public static String genTableKey(String schema, String tableName) {
        return String.format("%s,%s", schema, tableName);
    }

    public static Long sumRows(Long rowsFirst, Long rowsLast) {
        Long rowsF = rowsFirst == null ? 0 : rowsFirst;
        Long rowsL = rowsLast == null ? 0 : rowsLast;
        return rowsF + rowsL;
    }
}
