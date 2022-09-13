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

package com.alibaba.polardbx.qatest.dql.sharding.bloomfilter;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.google.common.base.Joiner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.dql.sharding.bloomfilter.DataTypeHelper.helperOf;
import static org.junit.Assert.assertTrue;

@Ignore
/**
 * 验证DN层BloomFilter UDF的正确性
 */
public class BloomFilterUdfTest {
    private static final Joiner COLUMN_JOINER = Joiner.on(",");
    private static final String[] INIT_SCRIPTS = new String[] {
        "DROP DATABASE IF EXISTS `bloomfilter_test`;",
        "CREATE DATABASE `bloomfilter_test`;",
        "CREATE TABLE `bloomfilter_test`.`t1`("
            + "`tiny_int_test` TINYINT NULL, "
            + "utiny_int_test TINYINT UNSIGNED NULL, "
            + "small_int_test SMALLINT NULL, "
            + "usmall_int_test SMALLINT UNSIGNED NULL, "
            + "medium_int_test MEDIUMINT NULL, "
            + "umedium_int_test MEDIUMINT UNSIGNED NULL, "
            + "int_test INT NULL, "
            + "uint_test INT UNSIGNED NULL, "
            + "big_int_test BIGINT NULL, "
            + "ubig_int_test BIGINT UNSIGNED NULL, "
            + "float_test FLOAT NULL, "
            + "double_test DOUBLE null, "
            + "decimal_test DECIMAL(40, 16) null"
            + ");"
    };

    private static final String[] UNINIT_SCRIPTS = new String[] {
    };

    private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE `bloomfilter_test`.`t1`";

    private static final DataTypeHelper[] COLUMN_HELPERS = new DataTypeHelper[] {
        helperOf(DataTypes.TinyIntType, "tiny_int_test"),
        helperOf(DataTypes.UTinyIntType, "utiny_int_test"),
        helperOf(DataTypes.SmallIntType, "small_int_test"),
        helperOf(DataTypes.USmallIntType, "usmall_int_test"),
        helperOf(DataTypes.MediumIntType, "medium_int_test"),
        helperOf(DataTypes.UMediumIntType, "umedium_int_test"),
        helperOf(DataTypes.IntegerType, "int_test"),
        helperOf(DataTypes.UIntegerType, "uint_test"),
        helperOf(DataTypes.LongType, "big_int_test"),
        helperOf(DataTypes.ULongType, "ubig_int_test"),
        helperOf(DataTypes.FloatType, "float_test"),
        helperOf(DataTypes.DoubleType, "double_test"),
        helperOf(DataTypes.DecimalType, "decimal_test")
    };

    private static final int DATA_SIZE = 1024;
    private static Connection mysqlConnection;
    private static boolean supportsBloomFilter = true;

    private Random random;
    private Chunk insertIntoMysqlData;
    private Chunk bloomFilterGeneratorData;
    private BloomFilterInfo[] bloomFilterInfos;
    private List<List<Boolean>> resultData;

    @BeforeClass
    public static void setUpResources() throws SQLException {
        mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
        if (mysqlConnection == null) {
            throw new IllegalStateException("Failed to create mysql connection!");
        }
        try (Statement stmt = mysqlConnection.createStatement()) {
            for (String sql : INIT_SCRIPTS) {
                stmt.executeUpdate(sql);
            }
        }
    }

    @AfterClass
    public static void cleanResources() throws Exception {
        if (mysqlConnection != null) {
            try (Statement stmt = mysqlConnection.createStatement()) {
                for (String sql : UNINIT_SCRIPTS) {
                    stmt.executeUpdate(sql);
                }
            } finally {
                mysqlConnection.close();
            }
        }
    }

    @Before
    public void setupData() throws SQLException {
        try (Statement stat = mysqlConnection.createStatement()) {
            stat.executeUpdate(TRUNCATE_TABLE_SQL);
            stat.execute("USE `bloomfilter_test`;");
        }

        random = new Random(DATA_SIZE * DATA_SIZE);
        resultData = new ArrayList<>();
    }

    @Test
    public void testBloomFilterExists() throws Exception {
        if (!supportsBloomFilter) {
            return;
        }
        insertIntoMysqlData = generateRandomChunk(true);
        bloomFilterGeneratorData = insertIntoMysqlData;

        doTest();

        for (int i = 0; i < resultData.size(); i++) {
            assertTrue(COLUMN_HELPERS[i].getColumnName() + " not match!",
                resultData.get(i).stream().allMatch(Objects::nonNull));
        }
    }

    @Test
    public void testBloomFilterNonExists() throws Exception {
        if (!supportsBloomFilter) {
            return;
        }
        insertIntoMysqlData = generateRandomChunk(false);
        bloomFilterGeneratorData = generateRandomChunk(true);

        doTest();

        for (int i = 0; i < resultData.size(); i++) {
            if (!COLUMN_HELPERS[i].needVerifyErrorRate()) {
                continue;
            }
            List<Boolean> result = resultData.get(i);
            long errorCount = result.stream().filter(c -> c).count();
            double errorRate = ((double) errorCount / result.size());
            double expectedRate = 0.1d;
            assertTrue(String
                    .format("%s error rate higher [%f] than expected [%f]", COLUMN_HELPERS[i].getColumnName(), errorRate,
                        expectedRate),
                errorRate <= expectedRate);
        }
    }

    private void doTest() throws Exception {
        resultData.clear();
        prepareBloomFilter();
        insertChunkToMysql(insertIntoMysqlData);
        computeResult();
    }

    private Chunk generateRandomChunk(boolean hasNull) {
        Block[] blocks = Arrays.stream(COLUMN_HELPERS)
            .map(h -> h.generateBlock(random, DATA_SIZE, hasNull))
            .toArray(Block[]::new);

        return new Chunk(blocks);
    }

    private void insertChunkToMysql(Chunk chunk) throws SQLException {
        String[] questionMarks = IntStream.range(0, COLUMN_HELPERS.length)
            .mapToObj(c -> "?")
            .toArray(String[]::new);

        String sql = "insert into `bloomfilter_test`.`t1` ("
            + COLUMN_JOINER
            .join(Arrays.stream(COLUMN_HELPERS).map(DataTypeHelper::getColumnName).toArray(String[]::new))
            + ") values ("
            + COLUMN_JOINER.join(questionMarks)
            + ");";

        try (PreparedStatement stmt = mysqlConnection.prepareStatement(sql)) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                Chunk.ChunkRow row = chunk.rowAt(i);
                for (int j = 0; j < chunk.getBlockCount(); j++) {
                    COLUMN_HELPERS[j].setPrepareStmt(stmt, j + 1, row.getObject(j));
                }
                stmt.executeUpdate();
            }
        }
    }

    private void prepareBloomFilter() {
        bloomFilterInfos = new BloomFilterInfo[bloomFilterGeneratorData.getBlockCount()];
        for (int i = 0; i < bloomFilterGeneratorData.getBlockCount(); i++) {
            List<Integer> hashColumns = Collections.singletonList(i);
            BloomFilter bloomFilter =
                BloomFilter.createEmpty(bloomFilterGeneratorData.getPositionCount());
            IStreamingHasher hasher = bloomFilter.newHasher();
            for (int pos = 0; pos < bloomFilterGeneratorData.getPositionCount(); pos++) {
                Chunk.ChunkRow row = bloomFilterGeneratorData.rowAt(pos);
                bloomFilter.put(row.hashCode(hasher, hashColumns));
            }
            bloomFilterInfos[i] = new BloomFilterInfo(0, bloomFilter.getBitmap(), bloomFilter.getNumHashFunctions(),
                bloomFilter.getHashMethodInfo(), new ArrayList<>());
        }
    }

    private void computeResult() throws SQLException {
        List<ParameterContext> parameters = RuntimeFilterDynamicParamInfo.fromRuntimeFilterId(0)
            .stream()
            .map(RuntimeFilterDynamicParamInfo::toParameterContext)
            .collect(Collectors.toList());

        for (int i = 0; i < insertIntoMysqlData.getBlockCount(); i++) {
            String sql = String.format("select BLOOMFILTER(?, ?, ?, `%s`) from `bloomfilter_test`.`t1`;",
                COLUMN_HELPERS[i].getColumnName());
            for (ParameterContext paramContext : parameters) {
                paramContext.getArgs()[RuntimeFilterDynamicParamInfo.ARG_IDX_BLOOM_FILTER_INFO] = bloomFilterInfos[i];
            }

            List<Boolean> result = new ArrayList<>(insertIntoMysqlData.getPositionCount());
            try (PreparedStatement stat = mysqlConnection.prepareStatement(sql)) {
                ParameterMethod.setParameters(stat, parameters);
                ResultSet resultSet = stat.executeQuery();
                while (resultSet.next()) {
                    result.add(resultSet.getBoolean(1));
                }
            }

            resultData.add(result);
        }
    }
}
