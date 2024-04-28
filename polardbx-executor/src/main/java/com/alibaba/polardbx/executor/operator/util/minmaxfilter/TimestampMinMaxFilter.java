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

package com.alibaba.polardbx.executor.operator.util.minmaxfilter;

import com.alibaba.polardbx.common.utils.bloomfilter.MinMaxFilterInfo;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.rpc.result.XResultUtil;

/**
 * @author chenzilin
 */
public class TimestampMinMaxFilter extends MinMaxFilter {
    private Long min;
    private Long max;

    public TimestampMinMaxFilter() {

    }

    public TimestampMinMaxFilter(Long min, Long max) {
        this.min = min;
        this.max = max;
    }

    public Long getMin() {
        return min;
    }

    public void setMin(Long min) {
        this.min = min;
    }

    public Long getMax() {
        return max;
    }

    public void setMax(Long max) {
        this.max = max;
    }

    @Override
    public void put(Block block, int pos) {
        if (!block.isNull(pos)) {
            OriginalTimestamp originalTimestamp = ((TimestampBlock) block).getTimestamp(pos);

            MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                originalTimestamp,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (mysqlDateTime == null) {
                return;
            }
            TimeParseStatus timeParseStatus = new TimeParseStatus();
            MySQLTimeVal timeVal =
                MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime, timeParseStatus,
                    originalTimestamp.getMysqlDateTime().getTimezone().toZoneId());
            if (timeVal == null) {
                return;
            }

            long num = XResultUtil.timeValToLong(timeVal);
            if (min == null || num < min) {
                min = num;
            }
            if (max == null || num > max) {
                max = num;
            }
        }
    }

    @Override
    public MinMaxFilterInfo toMinMaxFilterInfo() {
        return new MinMaxFilterInfo(
            MinMaxFilterInfo.TYPE.LONG,
            min == null ? null : min.longValue(),
            max == null ? null : max.longValue(), null, null, null, null, null, null);
    }

    @Override
    public Number getMinNumber() {
        return min;
    }

    @Override
    public Number getMaxNumber() {
        return max;
    }

    @Override
    public String getMinString() {
        return null;
    }

    @Override
    public String getMaxString() {
        return null;
    }
}

