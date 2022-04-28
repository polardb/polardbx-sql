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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import org.apache.orc.ColumnStatistics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class StripeColumnMeta {
    private OrcBloomFilter bloomFilter;
    private ColumnStatistics columnStatistics;
    private StripeInfo stripeInfo;

    public static OrcBloomFilter parseBloomFilter(ColumnMetasRecord record) {
        if (record == null) {
            return null;
        }
        if (record.isMerged == 0) {
            // compatible to old version column meta.
            return doParseUnmerged(record);
        } else {
            return doParseMerged(record);
        }
    }

    private static OrcBloomFilter doParseMerged(ColumnMetasRecord record) {
        // read bytes from specific offset
        Engine engine = Engine.valueOf(record.engine);
        int offset = (int) record.bloomFilterOffset;
        int length = (int) record.bloomFilterLength;
        byte[] buffer = new byte[length];
        FileSystemUtils.readFile(record.bloomFilterPath, offset, length, buffer, engine);

        // parse bloom filter from oss file.
        try {
            return OrcBloomFilter.deserialize(new ByteArrayInputStream(buffer, 0, length));
        } catch (Throwable t) {
            return null;
        }
    }

    private static OrcBloomFilter doParseUnmerged(ColumnMetasRecord record) {
        // parse bloom filter from oss file.
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FileSystemUtils.readFile(record.bloomFilterPath, byteArrayOutputStream, Engine.valueOf(record.engine));
        byte[] bytes = byteArrayOutputStream.toByteArray();
        try {
            return OrcBloomFilter.deserialize(new ByteArrayInputStream(bytes, 0, bytes.length));
        } catch (Throwable t) {
            return null;
        }
    }

    public long getStripeIndex() {
        return stripeInfo.getStripeIndex();
    }

    public long getStripeOffset() {
        return stripeInfo.getStripeOffset();
    }

    public long getStripeLength() {
        return stripeInfo.getStripeLength();
    }

    public OrcBloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(OrcBloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public ColumnStatistics getColumnStatistics() {
        return columnStatistics;
    }

    public void setColumnStatistics(ColumnStatistics columnStatistics) {
        this.columnStatistics = columnStatistics;
    }

    public void setStripeInfo(StripeInfo stripeInfo) {
        this.stripeInfo = stripeInfo;
    }
}
