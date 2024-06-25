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

package com.alibaba.polardbx.executor.test;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.mpp.execution.SessionRepresentation;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import com.alibaba.polardbx.executor.mpp.metadata.DefinedJsonSerde;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.weakref.jmx.internal.guava.collect.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import static com.alibaba.polardbx.common.utils.hash.HashMethodInfo.XXHASH_METHOD;

public class ParameterContextJSONTest {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void beforeClass() {
        objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        SimpleModule module1 = new SimpleModule();
        module1.addSerializer(ParameterContext.class, new DefinedJsonSerde.ParameterContextSerializer());
        module1.addDeserializer(ParameterContext.class, new DefinedJsonSerde.ParameterContextDeserializer());
        module1.addSerializer(RawString.class, new DefinedJsonSerde.RawStringSerializer());
        module1.addDeserializer(RawString.class, new DefinedJsonSerde.RawStringDeserializer());
        module1.addSerializer(PruneRawString.class, new DefinedJsonSerde.PruneRawStringSerializer());
        module1.addDeserializer(PruneRawString.class, new DefinedJsonSerde.PruneRawStringDeserializer());

        objectMapper.registerModule(module1);
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    public void testSession() throws Exception {
        ColumnarTracer columnarTracer = new ColumnarTracer();
        columnarTracer.tracePruneIndex("tbl", "a>10", 10, 10, 10);
        SessionRepresentation sessionRepresentation = new SessionRepresentation(
            "traceId",
            "catalog",
            "scheam",
            "user",
            "host",
            "encoding",
            "connstring",
            "sqlMode",
            4,
            1000,
            true,
            1000,
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashSet<>(),
            new HashMap<>(),
            new HashMap<>(),
            false,
            -1,
            new InternalTimeZone(TimeZone.getDefault(), "test"),
            1,
            false,
            new HashMap<>(),
            false,
            false,
            columnarTracer,
            WorkloadType.TP,
            null);

        String json = objectMapper.writeValueAsString(sessionRepresentation);
        System.out.println(json);
        SessionRepresentation target = objectMapper.readValue(json, sessionRepresentation.getClass());
        Assert.assertTrue(target.getDnLsnMap() != null);
        Assert.assertTrue(target.getColumnarTracer() != null);
        Assert.assertTrue(target.getColumnarTracer().pruneRecords().size() > 0);
    }

    @Test
    public void testJdbcSplit() throws Exception {
        ExecutionContext context = new ExecutionContext();
        context.setClientIp("127.1");
        context.setTraceId("testTrace");
        byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(context);
        String sql = "select pk from ? as tb1 where pk in (?)";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        JdbcSplit source =
            new JdbcSplit(
                "ca", "sc", "db0", hint, bytesSql, null, null, "127.1", null, ITransaction.RW.WRITE,
                true, 1L, null, false);

        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        JdbcSplit target = objectMapper.readValue(json, source.getClass());
        Assert.assertTrue(source.getHostAddress().equals(target.getHostAddress()));
    }

    @Test
    public void testSingleParameterContext() throws JsonProcessingException {
        ParameterContext source = new ParameterContext(ParameterMethod.setObject1, new Object[] {1, "1"});
        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        ParameterContext target = objectMapper.readValue(json, source.getClass());
        Assert.assertTrue(source.getValue().equals(target.getValue()));
    }

    @Test
    public void testSingleRawString() throws JsonProcessingException {
        RawString source = new RawString(Lists.newArrayList("123", 4.5f, "null", null));
        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        RawString target = objectMapper.readValue(json, source.getClass());
        Assert.assertTrue(target.getObjList().size() == source.getObjList().size());
    }

    @Test
    public void testRawStringListList() throws JsonProcessingException {
        List<Object> objectList = Lists.newArrayList("123", 4.5f, "null", null);
        List<List<Object>> objectListList = Lists.newArrayList(objectList, objectList);
        RawString source = new RawString(objectListList);
        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        RawString target = objectMapper.readValue(json, source.getClass());
        Assert.assertTrue(target.toString().equals(source.toString()));
    }

    @Test
    public void testPruneRawStringListList() throws JsonProcessingException {
        List<Object> objectList1 = Lists.newArrayList("123", 4.5f, "null", null);
        List<Object> objectList2 = Lists.newArrayList("1234", 4.6f, "null", null);
        List<Object> objectList3 = Lists.newArrayList("12345", 4.7f, "null", null);
        List<List<Object>> objectListList = Lists.newArrayList(objectList1, objectList2, objectList3);
        PruneRawString source = new PruneRawString(objectListList, PruneRawString.PRUNE_MODE.RANGE, 1, 2, null);
        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        PruneRawString target = objectMapper.readValue(json, source.getClass());
        System.out.println(source.toString());
        System.out.println(target.toString());
        Assert.assertTrue(target.getObjList().size() == 1);
        Assert.assertTrue(target.getObj(0, 0).equals("1234"));
        Assert.assertTrue(target.getObj(0, 1).toString().equals("4.6"));
        Assert.assertTrue(target.buildRawString().equals(source.buildRawString()));
    }

    @Test
    public void testNullRawString() throws JsonProcessingException {
        RawString source = null;
        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        RawString target = objectMapper.readValue(json, RawString.class);
        Assert.assertTrue(target == null);
    }

    @Test
    public void testMapParamContext() throws JsonProcessingException, JsonMappingException {
        Map<Integer, ParameterContext> map = new HashMap<>();
        ParameterContext source1 = new ParameterContext(ParameterMethod.setObject1, new Object[] {1, "1"});
        map.put(1, source1);
        ParameterContext source2 = new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 1});
        map.put(2, source2);
        String json = objectMapper.writeValueAsString(map);
        System.out.println(json);
        JavaType javaType =
            objectMapper.getTypeFactory().constructParametricType(Map.class, Integer.class, ParameterContext.class);
        Map<Integer, ParameterContext> target = objectMapper.readValue(json, javaType);
        Assert.assertTrue(map.size() == target.size());
    }

    @Test
    public void testRawString() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        RawString rawString1 = new RawString(Lists.newArrayList("123", 4.5f, "null", null));
        source.add(
            new ParameterContext(ParameterMethod.setObject1, new Object[] {1, rawString1}));
        RawString rawString2 = new RawString(Lists.newArrayList("123", "bb", "null", "aaa"));
        source.add(
            new ParameterContext(ParameterMethod.setObject1, new Object[] {1, rawString2}));
        testType(source);
    }

    @Test
    public void testNull() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            source.add(
                new ParameterContext(ParameterMethod.setObject1, new Object[] {1, null}));
        }
        testType(source);
    }

    @Test
    public void testSet() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Set<Integer> integerSet = Sets.newHashSet(i);
            source.add(
                new ParameterContext(ParameterMethod.setObject1, new Object[] {1, integerSet}));
        }
        testType(source);
    }

    @Test
    public void testStageId() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            StageId stageId = new StageId(i + "", i);
            source.add(
                new ParameterContext(ParameterMethod.setObject1, new Object[] {1, stageId, stageId}));
        }
        testType(source);
    }

    @Test
    public void testString() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, "aaa" + 1}));
        }
        testType(source);
    }

    @Test
    public void testShort() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, (short) 1}));
        }
        testType(source);
    }

    @Test
    public void testBoolean() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, i % 2 == 0 ? true : false}));
        }
        testType(source);
    }

    @Test
    public void testObjects() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, true}));
        source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 3.4d}));
        source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 3.4d, 4.3f, 1234L, "string"}));
        testType(source);
    }

    @Test
    public void testBigDecimal() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        BigDecimal decimal = new BigDecimal(12234345235.3453464576);
        for (int i = 0; i < 10; i++) {
            source.add(
                new ParameterContext(ParameterMethod.setObject1,
                    new Object[] {1, decimal.subtract(new BigDecimal(i))}));
        }
        testType(source);
    }

    @Test
    public void testDate() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        Date date = new Date(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, date}));
        }
        testType(source);
    }

    @Test
    public void testTime() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        Time time = new Time(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, time}));
        }
        testType(source);
    }

    @Test
    public void testTimestamp() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Timestamp time = new Timestamp(System.currentTimeMillis());
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, time}));
        }
        testType(source);
    }

    @Test
    public void testBytes() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            source.add(
                new ParameterContext(ParameterMethod.setObject1, new Object[] {1, new byte[] {(byte) i, (byte) i}}));
        }
        testType(source);
    }

    @Test
    public void testInt() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        Integer maxValue = Integer.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, maxValue - i}));
        }

        testType(source);
    }

    @Test
    public void testBoomFilter() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        Integer maxValue = Integer.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            BloomFilterInfo bloomFilterInfo = new BloomFilterInfo(i, new long[] {i},
                2, XXHASH_METHOD, new ArrayList<>());
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, bloomFilterInfo}));
        }

        testType(source);
    }

    @Test
    public void testLong() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        Long maxValue = 9223372036854775807L;
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, maxValue - i}));
        }
        testType(source);
    }

    @Test
    public void testUInt64() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        UInt64 uInt64 = new UInt64(Long.MAX_VALUE);
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, uInt64}));
        }
        testType(source);
    }

    @Test
    public void testByte() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte b = (byte) i;
            source.add(new ParameterContext(ParameterMethod.setObject1, new Object[] {1, b}));
        }
        testType(source);
    }

    @Test
    public void testBigInteger() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        BigInteger maxValue = new BigInteger("9223372036854775807");
        for (int i = 0; i < 10; i++) {
            source.add(new ParameterContext(ParameterMethod.setObject1,
                new Object[] {1, maxValue.add(maxValue)}));
        }
        testType(source);
    }

    @Test
    public void testFloat() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 114.1788f}));
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 114.1788234f}));
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 1234324514.17882234434f}));
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 114.1}));
        testType(source);
    }

    @Test
    public void testDouble() throws JsonProcessingException {
        List<ParameterContext> source = new ArrayList<>();
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 114.17882342345354d}));
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 114.17882323434d}));
        source.add(new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, 1234324514.17882234434d}));
        testType(source);
    }

    private void testType(List<ParameterContext> source) throws JsonProcessingException {
        testMapType(source);
        testListType(source);
    }

    private void testListType(List<ParameterContext> source) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(source);
        System.out.println(json);
        JavaType javaType =
            objectMapper.getTypeFactory().constructParametricType(ArrayList.class, ParameterContext.class);
        List<ParameterContext> target = objectMapper.readValue(json, javaType);
        verify(source, target);
    }

    private void testMapType(List<ParameterContext> source) throws JsonProcessingException {
        HashMap<Integer, ParameterContext> map = new HashMap<>();
        int index = 0;
        for (ParameterContext context : source) {
            map.put(index, context);
            index++;
        }
        String json = objectMapper.writeValueAsString(map);
        System.out.println(json);
        JavaType javaType =
            objectMapper.getTypeFactory().constructParametricType(TreeMap.class, Integer.class, ParameterContext.class);
        TreeMap<Integer, ParameterContext> target = objectMapper.readValue(json, javaType);
        List<ParameterContext> listTarget = new ArrayList<>();
        for (Map.Entry<Integer, ParameterContext> entry : target.entrySet()) {
            listTarget.add(entry.getValue());
        }
        verify(source, listTarget);
    }

    private void verify(List<ParameterContext> source, List<ParameterContext> target) {
        Assert.assertTrue(source.size() == target.size());
        for (int i = 0; i < source.size(); i++) {
            ParameterContext ret1 = source.get(i);
            ParameterContext ret2 = target.get(i);
            Assert.assertTrue(ret1.getArgs().length == ret2.getArgs().length);
            for (int j = 0; j < ret1.getArgs().length; j++) {
                Object[] objects1 = ret1.getArgs();
                Object[] objects2 = ret2.getArgs();
                if (objects1[j] == null) {
                    Assert.assertTrue(objects2[j] == null);
                } else if (objects1[j] instanceof Time) {
                    Assert.assertTrue(objects1[j].toString().equals(objects2[j].toString()));
                    Assert.assertTrue(objects1[j].getClass() == objects2[j].getClass());
                } else if (objects1[j] instanceof byte[]) {
                    Assert.assertTrue(objects1[j].getClass() == objects2[j].getClass());
                } else if (objects1[j] instanceof BloomFilterInfo) {
                    Assert.assertTrue(((BloomFilterInfo) objects1[j]).getDataLenInBits() ==
                        ((BloomFilterInfo) objects2[j]).getDataLenInBits());
                    Assert.assertTrue(objects1[j].getClass() == objects2[j].getClass());
                } else if (objects1[j] instanceof RawString) {
                    RawString rawStrings1 = (RawString) objects1[j];
                    RawString rawStrings2 = (RawString) objects2[j];
                    Assert.assertTrue(rawStrings1.toString().equals(rawStrings2.toString()));
                } else {
                    Assert.assertTrue(objects1[j].equals(objects2[j]));
                    Assert.assertTrue(objects1[j].getClass() == objects2[j].getClass());
                }

            }
        }
    }
}
