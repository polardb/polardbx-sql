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

package com.alibaba.polardbx.executor.operator.orcstatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class OrcStatisticsTest {
    String FILE_NAME = "/tmp/statisticsTest.orc";
    FileSystem FILE_SYSTEM;

    public static final Configuration configuration = new Configuration();
    public static final long rowGroupNum = 100;

    static {
        configuration.setLong("orc.row.index.stride", 100);
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

    /**
     * 构建全是null，repeating
     */
    private void buildVectorizedRow1(VectorizedRowBatch batch) {
        batch.reset();
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        field1.isRepeating = true;
        field1.noNulls = false;
        field1.isNull[0] = true;
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        field2.isRepeating = true;
        field2.noNulls = false;
        field2.isNull[0] = true;
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        field3.isRepeating = true;
        field3.noNulls = false;
        field3.isNull[0] = true;

        batch.size = batch.getMaxSize();
    }

    /**
     * 构建全是null，非repeating
     */
    private void buildVectorizedRow2(VectorizedRowBatch batch) {
        batch.reset();
        int maxSize = batch.getMaxSize();
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        field1.isRepeating = false;
        field2.isRepeating = false;
        field3.isRepeating = false;
        field1.noNulls = false;
        field2.noNulls = false;
        field3.noNulls = false;
        for (int i = 0; i < maxSize; i++) {
            field1.isNull[i] = true;
            field2.isNull[i] = true;
            field3.isNull[i] = true;
        }
        batch.size = maxSize;
    }

    /**
     * 构建中间存在数据为不为null，第一行和最后一行为null
     */
    private void buildVectorizedRow3(VectorizedRowBatch batch) {
        batch.reset();
        int maxSize = batch.getMaxSize();
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        field1.noNulls = false;
        field2.noNulls = false;
        field3.noNulls = false;
        for (int i = 0; i < maxSize; i++) {
            field1.isNull[i] = true;
            field2.isNull[i] = true;
            field3.isNull[i] = true;
        }
        int index = maxSize / 2;
        field1.isNull[index - 1] = false;
        field1.isNull[index] = false;
        field1.isNull[index + 1] = false;
        field1.vector[index - 1] = 1L;
        field1.vector[index] = 2L;
        field1.vector[index + 1] = 3L;

        field2.isNull[index - 1] = false;
        field2.isNull[index] = false;
        field2.isNull[index + 1] = false;
        field2.vector[index - 1] = 1.1;
        field2.vector[index] = 1.2;
        field2.vector[index + 1] = 1.3;

        field3.isNull[index - 1] = false;
        field3.isNull[index] = false;
        field3.isNull[index + 1] = false;
        field3.setVal(index - 1, "abc".getBytes());
        field3.setVal(index, "dd".getBytes());
        field3.setVal(index + 1, "ee".getBytes());

        batch.size = maxSize;
    }

    /**
     * 构建第一行为null，最后一行不为null
     */
    private void buildVectorizedRow4(VectorizedRowBatch batch) {
        batch.reset();
        int maxSize = batch.getMaxSize();
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        field1.noNulls = false;
        field2.noNulls = false;
        field3.noNulls = false;
        for (int i = 0; i < maxSize; i++) {
            field1.isNull[i] = true;
            field2.isNull[i] = true;
            field3.isNull[i] = true;
        }
        int index = maxSize - 1;

        field1.isNull[index] = false;
        field2.isNull[index] = false;
        field3.isNull[index] = false;
        field1.vector[index] = 4L;
        field2.vector[index] = 4.25;
        field3.setVal(index, "4dd".getBytes());

        batch.size = maxSize;
    }

    /**
     * 构建第一行不为null，最后一行为null
     */
    private void buildVectorizedRow5(VectorizedRowBatch batch) {
        batch.reset();
        int maxSize = batch.getMaxSize();
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        field1.noNulls = false;
        field2.noNulls = false;
        field3.noNulls = false;
        for (int i = 0; i < maxSize; i++) {
            field1.isNull[i] = true;
            field2.isNull[i] = true;
            field3.isNull[i] = true;
        }
        int index = 0;

        field1.isNull[index] = false;
        field2.isNull[index] = false;
        field3.isNull[index] = false;
        field1.vector[index] = 5L;
        field2.vector[index] = 5.25;
        field3.setVal(index, "5dd".getBytes());

        batch.size = maxSize;
    }

    /**
     * 构建第一行不为null，最后一行不为null，中间为null
     */
    private void buildVectorizedRow6(VectorizedRowBatch batch) {
        batch.reset();
        int maxSize = batch.getMaxSize();
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        field1.noNulls = false;
        field2.noNulls = false;
        field3.noNulls = false;
        for (int i = 0; i < maxSize; i++) {
            field1.isNull[i] = true;
            field2.isNull[i] = true;
            field3.isNull[i] = true;
        }
        int index = 0;

        field1.isNull[index] = false;
        field2.isNull[index] = false;
        field3.isNull[index] = false;
        field1.vector[index] = 6L;
        field2.vector[index] = 6.25;
        field3.setVal(index, "6cs".getBytes());

        index = maxSize - 1;

        field1.isNull[index] = false;
        field2.isNull[index] = false;
        field3.isNull[index] = false;
        field1.vector[index] = 61L;
        field2.vector[index] = 6.256;
        field3.setVal(index, "6cc".getBytes());

        batch.size = maxSize;
    }

    /**
     * 构建所有数据不为null
     */
    private void buildVectorizedRow7(VectorizedRowBatch batch) {
        batch.reset();
        int maxSize = batch.getMaxSize();
        Random random = new Random(0x6754L);
        LongColumnVector field1 = (LongColumnVector) batch.cols[0];
        DoubleColumnVector field2 = (DoubleColumnVector) batch.cols[1];
        BytesColumnVector field3 = (BytesColumnVector) batch.cols[2];
        for (int i = 0; i < maxSize; i++) {
            field1.vector[i] = random.nextLong();
            field2.vector[i] = random.nextDouble();
            field3.setVal(i, String.valueOf(random.nextInt(100000)).getBytes());
        }
        batch.size = maxSize;
    }

    private boolean compareByteAndString(byte[] bytes, String str) {
        if (bytes == null && str == null) {
            return true;
        }
        if (bytes == null || str == null) {
            return true;
        }
        return Arrays.equals(bytes, str.getBytes());
    }

    @Test
    public void statisticsTest() throws IOException {
        //写
        TypeDescription schema = TypeDescription.fromString("struct<field1:bigint,field2:double,field3:varchar(2000)>");
        VectorizedRowBatch batch = schema.createRowBatch((int) rowGroupNum);
        boolean[] recordFirstAndLatest = new boolean[schema.getMaximumId() + 1];
        Arrays.fill(recordFirstAndLatest, true);
        OrcFile.WriterOptions opts = OrcFile
            .writerOptions(configuration)
            .setSchema(schema)
            .fileSystem(FILE_SYSTEM)
            .recordFirstAndLatest(recordFirstAndLatest);
        Long[] field1 = new Long[14];
        Double[] field2 = new Double[14];
        byte[][] field3 = new byte[14][];
        try (Writer writer = OrcFile.createWriter(new Path(FILE_NAME), opts)) {
            //第一个rowGroup全为null
            buildVectorizedRow1(batch);
            writer.addRowBatch(batch);
            field1[0] = null;
            field1[1] = null;
            field2[0] = null;
            field2[1] = null;
            field3[0] = null;
            field3[1] = null;

            buildVectorizedRow2(batch);
            writer.addRowBatch(batch);
            field1[2] = null;
            field1[3] = null;
            field2[2] = null;
            field2[3] = null;
            field3[2] = null;
            field3[3] = null;

            buildVectorizedRow3(batch);
            writer.addRowBatch(batch);
            field1[4] = null;
            field1[5] = null;
            field2[4] = null;
            field2[5] = null;
            field3[4] = null;
            field3[5] = null;

            buildVectorizedRow4(batch);
            writer.addRowBatch(batch);
            field1[6] = null;
            field1[7] = ((LongColumnVector) batch.cols[0]).vector[batch.size - 1];
            field2[6] = null;
            field2[7] = ((DoubleColumnVector) batch.cols[1]).vector[batch.size - 1];
            BytesColumnVector bytesVector = (BytesColumnVector) batch.cols[2];
            field3[6] = null;
            field3[7] = Arrays.copyOfRange(bytesVector.vector[batch.size - 1], bytesVector.start[batch.size - 1],
                bytesVector.start[batch.size - 1] + bytesVector.length[batch.size - 1]);

            //切分stripe
            writer.writeIntermediateFooter();

            buildVectorizedRow5(batch);
            writer.addRowBatch(batch);
            field1[8] = ((LongColumnVector) batch.cols[0]).vector[0];
            field1[9] = null;
            field2[8] = ((DoubleColumnVector) batch.cols[1]).vector[0];
            field2[9] = null;
            field3[8] = Arrays.copyOfRange(bytesVector.vector[0], bytesVector.start[0],
                bytesVector.start[0] + bytesVector.length[0]);
            field3[9] = null;

            buildVectorizedRow6(batch);
            writer.addRowBatch(batch);
            field1[10] = ((LongColumnVector) batch.cols[0]).vector[0];
            field1[11] = ((LongColumnVector) batch.cols[0]).vector[batch.size - 1];
            field2[10] = ((DoubleColumnVector) batch.cols[1]).vector[0];
            field2[11] = ((DoubleColumnVector) batch.cols[1]).vector[batch.size - 1];
            field3[10] = Arrays.copyOfRange(bytesVector.vector[0], bytesVector.start[0],
                bytesVector.start[0] + bytesVector.length[0]);
            field3[11] = Arrays.copyOfRange(bytesVector.vector[batch.size - 1], bytesVector.start[batch.size - 1],
                bytesVector.start[batch.size - 1] + bytesVector.length[batch.size - 1]);

            buildVectorizedRow7(batch);
            writer.addRowBatch(batch);
            field1[12] = ((LongColumnVector) batch.cols[0]).vector[0];
            field1[13] = ((LongColumnVector) batch.cols[0]).vector[batch.size - 1];
            field2[12] = ((DoubleColumnVector) batch.cols[1]).vector[0];
            field2[13] = ((DoubleColumnVector) batch.cols[1]).vector[batch.size - 1];
            field3[12] = Arrays.copyOfRange(bytesVector.vector[0], bytesVector.start[0],
                bytesVector.start[0] + bytesVector.length[0]);
            field3[13] = Arrays.copyOfRange(bytesVector.vector[batch.size - 1], bytesVector.start[batch.size - 1],
                bytesVector.start[batch.size - 1] + bytesVector.length[batch.size - 1]);
        }

        //读
        OrcFile.ReaderOptions readOptions = OrcFile
            .readerOptions(configuration)
            .filesystem(FILE_SYSTEM);

        Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions);

        RecordReaderImpl recordReader = (RecordReaderImpl) reader.rows();

        //文件级别统计信息
        ColumnStatistics[] fileStatistics = reader.getStatistics();
        IntegerColumnStatistics field1Stat = (IntegerColumnStatistics) fileStatistics[1];
        DoubleColumnStatistics field2Stat = (DoubleColumnStatistics) fileStatistics[2];
        StringColumnStatistics field3Stat = (StringColumnStatistics) fileStatistics[3];

        Assert.assertEquals(field1[0], field1Stat.getFirst());
        Assert.assertEquals(field1[13], field1Stat.getLatest());
        Assert.assertEquals(field2[0], field2Stat.getFirst());
        Assert.assertEquals(field2[13], field2Stat.getLatest());
        Assert.assertTrue(compareByteAndString(field3[0], field3Stat.getFirst()));
        Assert.assertTrue(compareByteAndString(field3[13], field3Stat.getLatest()));

        //stripe级别
        Assert.assertEquals(2, reader.getStripes().size());
        List<StripeStatistics> stripes = reader.getStripeStatistics();
        Assert.assertEquals(2, stripes.size());
        ColumnStatistics[] stripStatistics = stripes.get(0).getColumnStatistics();
        field1Stat = (IntegerColumnStatistics) stripStatistics[1];
        field2Stat = (DoubleColumnStatistics) stripStatistics[2];
        field3Stat = (StringColumnStatistics) stripStatistics[3];

        Assert.assertEquals(field1[0], field1Stat.getFirst());
        Assert.assertEquals(field1[7], field1Stat.getLatest());
        Assert.assertEquals(field2[0], field2Stat.getFirst());
        Assert.assertEquals(field2[7], field2Stat.getLatest());
        Assert.assertTrue(compareByteAndString(field3[0], field3Stat.getFirst()));
        Assert.assertTrue(compareByteAndString(field3[7], field3Stat.getLatest()));

        stripStatistics = stripes.get(1).getColumnStatistics();
        field1Stat = (IntegerColumnStatistics) stripStatistics[1];
        field2Stat = (DoubleColumnStatistics) stripStatistics[2];
        field3Stat = (StringColumnStatistics) stripStatistics[3];

        Assert.assertEquals(field1[8], field1Stat.getFirst());
        Assert.assertEquals(field1[13], field1Stat.getLatest());
        Assert.assertEquals(field2[8], field2Stat.getFirst());
        Assert.assertEquals(field2[13], field2Stat.getLatest());
        Assert.assertTrue(compareByteAndString(field3[8], field3Stat.getFirst()));
        Assert.assertTrue(compareByteAndString(field3[13], field3Stat.getLatest()));

        //rowGroup级别
        Long[] field1Read = new Long[14];
        Double[] field2Read = new Double[14];
        String[] field3Read = new String[14];
        List<StripeInformation> stripeInformation = reader.getStripes();
        int readIndex = 0;
        for (int stripeIndex = 0; stripeIndex < stripes.size(); stripeIndex++) {
            StripeInformation stripe = stripeInformation.get(stripeIndex);
            long rowCountInStripe = stripe.getNumberOfRows();
            long rowIndexNumInStripe = reader.getRowIndexStride();
            long rowGroupNum = ((rowCountInStripe + rowIndexNumInStripe - 1) / rowIndexNumInStripe);
            OrcIndex orcIndex = recordReader.readRowIndex(stripeIndex, null, null);
            OrcProto.RowIndex[] indexes = orcIndex.getRowGroupIndex();
            for (int rowGroup = 0; rowGroup < rowGroupNum; rowGroup++) {
                OrcProto.ColumnStatistics orcProtoData = indexes[1].getEntry(rowGroup).getStatistics();
                field1Stat = (IntegerColumnStatistics) ColumnStatisticsImpl.deserialize(null, orcProtoData);
                orcProtoData = indexes[2].getEntry(rowGroup).getStatistics();
                field2Stat = (DoubleColumnStatistics) ColumnStatisticsImpl.deserialize(null, orcProtoData);
                orcProtoData = indexes[3].getEntry(rowGroup).getStatistics();
                field3Stat = (StringColumnStatistics) ColumnStatisticsImpl.deserialize(null, orcProtoData);

                field1Read[readIndex] = field1Stat.getFirst();
                field2Read[readIndex] = field2Stat.getFirst();
                field3Read[readIndex] = field3Stat.getFirst();
                readIndex++;
                field1Read[readIndex] = field1Stat.getLatest();
                field2Read[readIndex] = field2Stat.getLatest();
                field3Read[readIndex] = field3Stat.getLatest();
                readIndex++;
            }
        }

        Assert.assertArrayEquals(field1, field1Read);
        Assert.assertArrayEquals(field2, field2Read);
        for (int i = 0; i < field3.length; i++) {
            Assert.assertTrue(compareByteAndString(field3[i], field3Read[i]));
        }

        reader.close();
        recordReader.close();

    }
}
