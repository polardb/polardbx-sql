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

package com.alibaba.polardbx.executor.operator.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.sarg.PredicateLeaf;
import org.apache.orc.sarg.SearchArgument;
import org.apache.orc.sarg.SearchArgumentFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class OrcReadByRowGroupTest {
    String FILE_NAME = "/tmp/readRowGroupTest.orc";
    FileSystem FILE_SYSTEM;

    private final TypeDescription schema =
        TypeDescription.fromString("struct<field1:bigint,field2:bigint,field3:varchar(1000)>");

    private static final Configuration configuration = new Configuration();

    static {
        OrcConf.ROW_INDEX_STRIDE.setInt(configuration, 10000);
        OrcConf.STRIPE_SIZE.setLong(configuration, 10L * 1024 * 1024);
        //OrcConf.BUFFER_SIZE.setLong(configuration, 64 * 1024);
        OrcConf.COMPRESS.setString(configuration, "LZ4");
    }

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

    private long buildFile(long rowNum) throws IOException {
        final Long[] writeLen = {0L};
        VectorizedRowBatch batch = schema.createRowBatch();
        OrcFile.WriterOptions opts = OrcFile
            .writerOptions(configuration)
            .setSchema(schema)
            .fileSystem(FILE_SYSTEM)
            .rateLimiter((len) ->
                writeLen[0] += len
            );
        Random random = new Random(2024);
        byte[] field3Buf = new byte[100];
        try (Writer writer = OrcFile.createWriter(new Path(FILE_NAME), opts)) {
            final int batchSize = batch.getMaxSize();
            for (int i = 0; i < rowNum; i++) {
                int row = batch.size;
                LongColumnVector field1 = (LongColumnVector) batch.cols[0];
                field1.vector[row] = i;
                LongColumnVector field2 = (LongColumnVector) batch.cols[1];
                random.setSeed(i);
                field2.vector[row] = random.nextLong();
                BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
                random.setSeed(i);
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
        return fileLen;
    }

    @Test
    public void readTest() throws IOException {
        long rowNum = 1000000L;
        long fileLen = buildFile(rowNum);

        //读
        readAllData(rowNum, fileLen, false);
        readAllData(rowNum, fileLen, true);

        //seek 读
        readSeekData(rowNum, fileLen, false);
        readSeekData(rowNum, fileLen, true);

        //search 读
        readSearchData(rowNum, fileLen, false);
        readSearchData(rowNum, fileLen, true);
    }

    private void readAllData(long rowNum, long fileLen, boolean readByRowGroup) throws IOException {
        //读
        final Long[] readLen = {0L, 0L};
        OrcFile.ReaderOptions readOptions = OrcFile
            .readerOptions(configuration)
            .filesystem(FILE_SYSTEM)
            .setRateLimiter((len) -> {
                readLen[0] += len;
                readLen[1]++;
                //System.out.println("read IO: " + len);
            });

        if (readByRowGroup) {
            readOptions.setReadStripeByRowGroup(true);
        }

        Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions);
        RecordReader recordReader = reader.rows();

        //1. 顺序读
        VectorizedRowBatch batch = schema.createRowBatch();

        long rows = 0;
        Random random = new Random();
        byte[] field3Buf = new byte[100];
        while (recordReader.nextBatch(batch)) {
            //System.out.println("read row: " + batch.size);
            Assert.assertEquals(rows, ((LongColumnVector) batch.cols[0]).vector[0]);
            random.setSeed(rows);
            Assert.assertEquals(random.nextLong(), ((LongColumnVector) batch.cols[1]).vector[0]);
            random.setSeed(rows);
            random.nextBytes(field3Buf);
            BytesColumnVector bytesVector = ((BytesColumnVector) batch.cols[2]);
            byte[] field3 = Arrays.copyOfRange(bytesVector.vector[0], bytesVector.start[0],
                bytesVector.start[0] + bytesVector.length[0]);
            Assert.assertArrayEquals(field3Buf, field3);

            rows += batch.size;
            batch.reset();
        }

        Assert.assertEquals(rowNum, rows);
        System.out.println(FILE_NAME + " fileLen: " + fileLen + " readLen: " + readLen[0] + " IONum: " + readLen[1]);

        recordReader.close();
        reader.close();
    }

    private void readSeekData(long rowNum, long fileLen, boolean readByRowGroup) throws IOException {
        //读
        final Long[] readLen = {0L, 0L};
        OrcFile.ReaderOptions readOptions = OrcFile
            .readerOptions(configuration)
            .filesystem(FILE_SYSTEM)
            .setRateLimiter((len) -> {
                readLen[0] += len;
                readLen[1]++;
                //System.out.println("read IO: " + len);
            });

        if (readByRowGroup) {
            readOptions.setReadStripeByRowGroup(true);
        }

        Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions);
        RecordReader recordReader = reader.rows();

        //1. 顺序读
        VectorizedRowBatch batch = schema.createRowBatch();

        long rows = 0;
        Random random = new Random();
        byte[] field3Buf = new byte[100];

        //seek位置
        int[] seekPos = new int[] {200000, 100000, 178762, 703232};
        //seek条件 > 该值时
        int[] seekDo = new int[] {400000, 800000, 778762, 603232};

        int seekCurrent = 0;

        while (recordReader.nextBatch(batch)) {
            //System.out.println("read row: " + batch.size);
            Assert.assertEquals(rows, ((LongColumnVector) batch.cols[0]).vector[0]);
            random.setSeed(rows);
            Assert.assertEquals(random.nextLong(), ((LongColumnVector) batch.cols[1]).vector[0]);
            random.setSeed(rows);
            random.nextBytes(field3Buf);
            BytesColumnVector bytesVector = ((BytesColumnVector) batch.cols[2]);
            byte[] field3 = Arrays.copyOfRange(bytesVector.vector[0], bytesVector.start[0],
                bytesVector.start[0] + bytesVector.length[0]);
            Assert.assertArrayEquals(field3Buf, field3);

            rows += batch.size;
            batch.reset();
            if (seekCurrent < seekPos.length && rows > seekDo[seekCurrent]) {
                recordReader.seekToRow(seekPos[seekCurrent]);
                rows = seekPos[seekCurrent];
                seekCurrent++;
            }
        }

        Assert.assertEquals(rowNum, rows);
        System.out.println(FILE_NAME + " fileLen: " + fileLen + " readLen: " + readLen[0] + " IONum: " + readLen[1]);

        recordReader.close();
        reader.close();
    }

    private void readSearchData(long rowNum, long fileLen, boolean readByRowGroup) throws IOException {
        //读
        final Long[] readLen = {0L, 0L};
        OrcFile.ReaderOptions readOptions = OrcFile
            .readerOptions(configuration)
            .filesystem(FILE_SYSTEM)
            .setRateLimiter((len) -> {
                readLen[0] += len;
                readLen[1]++;
                //System.out.println("read IO: " + len);
            });

        if (readByRowGroup) {
            readOptions.setReadStripeByRowGroup(true);
        }

        Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions);
        Reader.Options options = new Reader.Options();
        options.searchArgument(SearchArgumentFactory.newBuilder().literal(SearchArgument.TruthValue.YES)
            .between("field1", PredicateLeaf.Type.LONG, 100000L, 200000L).build(), new String[] {"field1"});
        RecordReader recordReader = reader.rows(options);

        //1. 顺序读
        VectorizedRowBatch batch = schema.createRowBatch();

        long rows = 0;
        Random random = new Random();
        byte[] field3Buf = new byte[100];
        while (recordReader.nextBatch(batch)) {
            //System.out.println("read row: " + batch.size);
            long seed = ((LongColumnVector) batch.cols[0]).vector[0];

            random.setSeed(seed);
            Assert.assertEquals(random.nextLong(), ((LongColumnVector) batch.cols[1]).vector[0]);
            random.setSeed(seed);
            random.nextBytes(field3Buf);
            BytesColumnVector bytesVector = ((BytesColumnVector) batch.cols[2]);
            byte[] field3 = Arrays.copyOfRange(bytesVector.vector[0], bytesVector.start[0],
                bytesVector.start[0] + bytesVector.length[0]);
            Assert.assertArrayEquals(field3Buf, field3);

            rows += batch.size;
            batch.reset();
        }

        Assert.assertTrue(rows >= 100000L);
        System.out.println("read rows: " + rows);
        System.out.println(FILE_NAME + " fileLen: " + fileLen + " readLen: " + readLen[0] + " IONum: " + readLen[1]);

        recordReader.close();
        reader.close();
    }
}
