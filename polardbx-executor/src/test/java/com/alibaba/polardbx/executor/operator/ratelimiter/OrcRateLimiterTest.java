/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.ratelimiter;

import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.OrcTail;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class OrcRateLimiterTest {
    String FILE_NAME = "/tmp/rateLimiterTest.orc";
    FileSystem FILE_SYSTEM;

    @Before
    public void before() throws IOException {
        FILE_SYSTEM = FileSystem.getLocal(new Configuration());
        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }
    }

    @After
    public void after() throws IOException {
        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }
    }

    @Test
    public void rateLimiterTest() throws IOException {
        //写
        final Long[] writeLen = {0L};
        long rowNum = 1000000L;
        TypeDescription schema = TypeDescription.fromString("struct<field1:bigint,field2:bigint,field3:varchar(1000)>");
        VectorizedRowBatch batch = schema.createRowBatch();
        OrcFile.WriterOptions opts = OrcFile
            .writerOptions(new Configuration())
            .setSchema(schema)
            .fileSystem(FILE_SYSTEM)
            .rateLimiter((len) ->
                writeLen[0] += len
            );
        Random random = new Random(2024);
        byte[] field3Buf = new byte[20];
        try (Writer writer = OrcFile.createWriter(new Path(FILE_NAME), opts)) {
            final int batchSize = batch.getMaxSize();
            for (int i = 0; i < rowNum; i++) {
                int row = batch.size;
                LongColumnVector field1 = (LongColumnVector) batch.cols[0];
                field1.vector[row] = i;
                LongColumnVector field2 = (LongColumnVector) batch.cols[1];
                field2.vector[row] = i + 1000L;
                BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
                random.nextBytes(field3Buf);
                field3.setVal(row, field3Buf);
                batch.size++;
                if (batch.size == batchSize) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        long fileLen = FILE_SYSTEM.getFileStatus(new Path(FILE_NAME)).getLen();

        System.out.println(FILE_NAME + " fileLen: " + fileLen + " writeLen: " + writeLen[0]);
        Assert.assertEquals(fileLen, (long) writeLen[0]);

        //读
        final Long[] readLen = {0L};
        OrcFile.ReaderOptions readOptions = OrcFile
            .readerOptions(new Configuration())
            .filesystem(FILE_SYSTEM)
            .setRateLimiter((len) -> {
                readLen[0] += len;
                //System.out.println("read: " + len);
            });

        Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions);

        RecordReader recordReader = reader.rows();

        long rows = 0;
        while (recordReader.nextBatch(batch)) {
            rows += batch.size;
            batch.reset();
        }

        Assert.assertEquals(rowNum, rows);
        System.out.println(FILE_NAME + " fileLen: " + fileLen + " readLen: " + readLen[0]);

        ByteBuffer tail = reader.getSerializedFileFooter();
        OrcTail orcTail = OrcMetaUtils.extractFileTail(tail);

        recordReader.close();
        reader.close();

        //已包含尾部元数据情况下读
        readLen[0] = 0L;
        readOptions = OrcFile
            .readerOptions(new Configuration())
            .filesystem(FILE_SYSTEM)
            .orcTail(orcTail)
            .setRateLimiter((len) -> {
                readLen[0] += len;
                //System.out.println("read: " + len);
            });

        reader = OrcFile.createReader(new Path(FILE_NAME), readOptions);

        recordReader = reader.rows();

        rows = 0;
        while (recordReader.nextBatch(batch)) {
            rows += batch.size;
            batch.reset();
        }

        Assert.assertEquals(rowNum, rows);
        System.out.println(FILE_NAME + " fileLen: " + fileLen + " no tail readLen: " + readLen[0]);
        recordReader.close();
        reader.close();

    }
}
