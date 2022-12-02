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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.hash.HashMethodInfo;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
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
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

@Ignore
/**
 * 验证DN层BloomFilter UDF的正确性
 */
public class BloomFilterUdfTest extends BaseTestCase {
    private static final Log logger = LogFactory.getLog(BloomFilterUdfTest.class);

    private static final String DB_NAME = "bloomfilter_test";
    private static final String TABLE_NAME = "t1";
    private static final Joiner COLUMN_JOINER = Joiner.on(",");

    private static final DataTypeHelper[] COLUMN_HELPERS = new DataTypeHelper[] {
        new DataTypeHelper(DataTypes.TinyIntType, "tiny_int_test", "TINYINT"),
        new DataTypeHelper(DataTypes.UTinyIntType, "utiny_int_test", "TINYINT UNSIGNED"),
        new DataTypeHelper(DataTypes.SmallIntType, "small_int_test", "SMALLINT"),
        new DataTypeHelper(DataTypes.USmallIntType, "usmall_int_test", "SMALLINT UNSIGNED"),
        new DataTypeHelper(DataTypes.MediumIntType, "medium_int_test", "MEDIUMINT"),
        new DataTypeHelper(DataTypes.UMediumIntType, "umedium_int_test", "MEDIUMINT UNSIGNED"),
        new DataTypeHelper(DataTypes.IntegerType, "int_test", "INT"),
        new DataTypeHelper(DataTypes.UIntegerType, "uint_test", "INT UNSIGNED"),
        new DataTypeHelper(DataTypes.LongType, "big_int_test", "BIGINT"),
        new DataTypeHelper(DataTypes.ULongType, "ubig_int_test", "BIGINT UNSIGNED"),
        new DataTypeHelper(DataTypes.FloatType, "float_test", "FLOAT"),
        new DataTypeHelper(DataTypes.DoubleType, "double_test", "DOUBLE"),
        new DataTypeHelper(new CharType(CharsetName.LATIN1, CollationName.LATIN1_GENERAL_CI),
            "latin_char_test", "CHAR(40) CHARACTER SET latin1"),
        new DataTypeHelper(DataTypes.CharType, "utf8_char_test", "CHAR(40) CHARACTER SET utf8"),
        new DataTypeHelper(new CharType(CharsetName.GBK, CollationName.GBK_CHINESE_CI),
            "gbk_char_test", "CHAR(40) CHARACTER SET GBK COLLATE GBK_CHINESE_CI"),
        new DataTypeHelper(DataTypes.VarcharType, "utf8_varchar_test", "VARCHAR(40) CHARACTER SET utf8"),
        new DataTypeHelper(DataTypes.DateType, "date_test", "DATE"),
        new DataTypeHelper(DataTypes.DatetimeType, "datetime_test", "DATETIME"),
        new DataTypeHelper(DataTypes.TimestampType, "timestamp_test", "TIMESTAMP"),
        new DataTypeHelper(DataTypes.TimeType, "time_test", "TIME"),
        // Decimal not supported
//        new DataTypeHelper(DataTypes.DecimalType, "decimal_test", "DECIMAL(40, 16)"),
    };
    private static final int COLUMN_COUNT = COLUMN_HELPERS.length;
    private static final String[] TEST_COLUMN_DEFS = new String[COLUMN_COUNT];

    static {
        for (int i = 0; i < COLUMN_COUNT; i++) {
            TEST_COLUMN_DEFS[i] = String.format("`%s` %s NULL", COLUMN_HELPERS[i].getColumnName(),
                COLUMN_HELPERS[i].getColumnDef());
        }
    }

    private static final String CREATE_TABLE_SQL = String.format("CREATE TABLE `%s`.`%s` (%s);",
        DB_NAME, TABLE_NAME, StringUtils.join(TEST_COLUMN_DEFS, ","));
    private static final String TRUNCATE_TABLE_SQL = String.format("TRUNCATE TABLE `%s`.`%s`", DB_NAME, TABLE_NAME);

    /**
     * 兼容性老版本udf
     */
    private static final String DEFAULT_BLOOMFILTER_SQL =
        String.format("select BLOOMFILTER(?, ?, ?, `%%s`) from `%s`.`%s`;", DB_NAME, TABLE_NAME);
    private static final String XXHASH_BLOOMFILTER_SQL =
        String.format("select BLOOMFILTER(?, ?, ?, `%%s`, 1) from `%s`.`%s`;", DB_NAME, TABLE_NAME);

    private static final int DATA_SIZE = 1024;
    private static final double EXPECTED_ERROR_RATE = 0.03d;

    private static Connection mysqlConnection;

    private Random random;
    private Chunk insertIntoMysqlData;
    private Chunk bloomFilterGeneratorData;
    private BloomFilterInfo[] bloomFilterInfos;
    private List<List<Boolean>> resultData;
    private static boolean useXxHash = false;

    @BeforeClass
    public static void setUpResources() throws SQLException {
        mysqlConnection = ConnectionManager.getInstance().newMysqlConnection();
        if (mysqlConnection == null) {
            throw new IllegalStateException("Failed to create mysql connection!");
        }
        try (Statement stmt = mysqlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("show variables like 'udf_bloomfilter_xxhash'")) {
            useXxHash = rs.next();
        }
        logger.info("Using xxhash: " + useXxHash);

        JdbcUtil.dropDatabase(mysqlConnection, DB_NAME);
        JdbcUtil.createDatabase(mysqlConnection, DB_NAME, "");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, CREATE_TABLE_SQL);
    }

    @AfterClass
    public static void cleanResources() {
        JdbcUtil.dropDatabase(mysqlConnection, DB_NAME);
        JdbcUtil.close(mysqlConnection);
    }

    @Before
    public void setupData() {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, TRUNCATE_TABLE_SQL);
        JdbcUtil.useDb(mysqlConnection, DB_NAME);

        random = new Random(System.currentTimeMillis());
        resultData = new ArrayList<>();
    }

    /**
     * 验证数据一定存在的情况
     */
    @Test
    public void testBloomFilterExists() throws Exception {
        if (isGalaxy()) {
            return;
        }

        insertIntoMysqlData = generateRandomChunk(true);
        bloomFilterGeneratorData = insertIntoMysqlData;

        doTest();
        Assert.assertEquals("Wrong result column count", resultData.size(), COLUMN_COUNT);
        List<String> errorMsgs = new ArrayList<>();
        for (int i = 0; i < COLUMN_COUNT; i++) {
            if (!resultData.get(i).stream().allMatch(b -> b != null && b)) {
                errorMsgs.add(String
                    .format("Bloomfilter result of column %s does not not match!", COLUMN_HELPERS[i].getColumnName()));
            }
        }
        Assert.assertTrue(StringUtils.join(errorMsgs, "\n"), errorMsgs.isEmpty());
    }

    /**
     * 验证数据不存在的情况
     * 错误率需要低于指定阈值
     */
    @Test
    public void testBloomFilterNonExists() throws Exception {
        if (isGalaxy()) {
            return;
        }

        insertIntoMysqlData = generateRandomChunk(false);
        bloomFilterGeneratorData = generateRandomChunk(true);

        doTest();
        Assert.assertEquals("Wrong result column count", resultData.size(), COLUMN_COUNT);
        List<String> errorMsgs = new ArrayList<>();
        for (int i = 0; i < COLUMN_COUNT; i++) {
            if (!COLUMN_HELPERS[i].needVerifyErrorRate()) {
                continue;
            }
            List<Boolean> result = resultData.get(i);
            long errorCount = result.stream().filter(c -> c).count();
            double errorRate = ((double) errorCount / result.size());
            double expectedRate = EXPECTED_ERROR_RATE;
            if (COLUMN_HELPERS[i].needAmplifyErrorRate()) {
                // 0.03 -> 0.05
                expectedRate *= 1.7;
            } else {
                // 0.03 -> 0.039
                expectedRate *= 1.3;
            }
            if (errorRate > expectedRate) {
                errorMsgs.add(String.format("%s error rate higher [%f] than expected [%f]",
                    COLUMN_HELPERS[i].getColumnName(), errorRate, expectedRate));
            }
        }
        Assert.assertTrue(StringUtils.join(errorMsgs, "\n"), errorMsgs.isEmpty());
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
        String questionMarks = COLUMN_JOINER.join(IntStream.range(0, COLUMN_HELPERS.length)
            .mapToObj(c -> "?")
            .toArray(String[]::new));
        String columns = COLUMN_JOINER.join(Arrays.stream(COLUMN_HELPERS)
            .map(DataTypeHelper::getColumnName)
            .toArray(String[]::new));
        String sql = String.format("insert into `%s`.`%s` (%s) values (%s)", DB_NAME, TABLE_NAME,
            columns, questionMarks);
        try (PreparedStatement stmt = mysqlConnection.prepareStatement(sql)) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                Chunk.ChunkRow row = chunk.rowAt(i);
                for (int j = 0; j < COLUMN_COUNT; j++) {
                    if (chunk.getBlock(j) instanceof SliceBlock) {
                        COLUMN_HELPERS[j].setSlicePrepareStmt(stmt, j + 1, (Slice) row.getObject(j));
                    } else {
                        COLUMN_HELPERS[j].setPrepareStmt(stmt, j + 1, row.getObject(j));
                    }
                }
                stmt.executeUpdate();
            }
        }
    }

    /**
     * 根据DN支持版本创建基于Murmur3或XxHash的BloomFilter
     */
    private void prepareBloomFilter() {
        bloomFilterInfos = new BloomFilterInfo[COLUMN_COUNT];
        for (int i = 0; i < COLUMN_COUNT; i++) {
            List<Integer> hashColumns = Collections.singletonList(i);
            BloomFilter bloomFilter = createBloomFilter();
            IStreamingHasher hasher = bloomFilter.newHasher();
            for (int pos = 0; pos < bloomFilterGeneratorData.getPositionCount(); pos++) {
                Chunk.ChunkRow row = bloomFilterGeneratorData.rowAt(pos);
                bloomFilter.put(row.hashCode(hasher, hashColumns));
            }
            bloomFilterInfos[i] = new BloomFilterInfo(0, bloomFilter.getBitmap(), bloomFilter.getNumHashFunctions(),
                bloomFilter.getHashMethodInfo(), new ArrayList<>());
        }
    }

    private BloomFilter createBloomFilter() {
        return BloomFilter.createEmpty(useXxHash ? HashMethodInfo.XXHASH_METHOD : HashMethodInfo.MURMUR3_METHOD,
            bloomFilterGeneratorData.getPositionCount(), EXPECTED_ERROR_RATE);
    }

    private void computeResult() throws SQLException {
        List<ParameterContext> parameters = RuntimeFilterDynamicParamInfo.fromRuntimeFilterId(0)
            .stream()
            .map(RuntimeFilterDynamicParamInfo::toParameterContext)
            .collect(Collectors.toList());

        for (int i = 0; i < COLUMN_COUNT; i++) {
            String sql = String.format(useXxHash ? XXHASH_BLOOMFILTER_SQL : DEFAULT_BLOOMFILTER_SQL,
                COLUMN_HELPERS[i].getColumnName());
            for (ParameterContext paramContext : parameters) {
                paramContext.getArgs()[RuntimeFilterDynamicParamInfo.ARG_IDX_BLOOM_FILTER_INFO] = bloomFilterInfos[i];
            }

            List<Boolean> result = new ArrayList<>(insertIntoMysqlData.getPositionCount());
            ResultSet resultSet = null;
            try (PreparedStatement stmt = mysqlConnection.prepareStatement(sql)) {
                ParameterMethod.setParameters(stmt, parameters);
                resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    result.add(resultSet.getBoolean(1));
                }
            } finally {
                JdbcUtil.close(resultSet);
            }

            resultData.add(result);
        }
    }
}
