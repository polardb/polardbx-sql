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

package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * @author shengyu
 */
public class HistogramTest {
    /**
     * test binary search
     */
    @Test
    public void testFindBucket() throws Exception {
        IntegerType it = new IntegerType();

        Histogram h = new Histogram(7, it, 1);
        Integer[] list = new Integer[10000];
        Random r1 = new Random();
        for (int i = 0; i < list.length; i++) {
            list[i] = r1.nextInt(list.length * 100);
        }
        h.buildFromData(list);

        Method format = h.getClass().getDeclaredMethod("findBucket", Object.class, boolean.class);
        format.setAccessible(true);
        for (int i : list) {
            //i = i - 10;
            Histogram.Bucket bucket = (Histogram.Bucket) format.invoke(h, i, true);
            if (bucket == null) {
                continue;
            }
            Assert.assertTrue(Double.parseDouble(bucket.getUpper().toString()) >= i);
        }
    }

    /**
     * test histogram without continuation value
     */
    @Test
    public void testFindBucketSpecial() throws Exception {
        IntegerType it = new IntegerType();

        Histogram h = new Histogram(7, it, 1);
        Integer[] list = new Integer[10];
        list[0] = 1;
        list[1] = 1;
        list[2] = 1;
        list[3] = 1;
        list[4] = 3;
        list[5] = 3;
        list[6] = 3;
        list[7] = 3;
        list[8] = 3;
        h.buildFromData(list);

        Method format = h.getClass().getDeclaredMethod("findBucket", Object.class, boolean.class);
        format.setAccessible(true);
        Histogram.Bucket bucket = (Histogram.Bucket) format.invoke(h, 1, true);
        Assert.assertTrue(bucket != null);
        bucket = (Histogram.Bucket) format.invoke(h, 2, true);
        Assert.assertTrue(bucket == null);
    }

    /**
     * test histogram without continuation value
     */
    @Test
    public void testFindBucketSpecial1() throws Exception {
        String serialize =
            "{\"buckets\":[{\"ndv\":1200,\"upper\":-3974432282297842208,\"lower\":-9222262852446475180,\"count\":1200,\"preSum\":0},"
                + "{\"ndv\":1200,\"upper\":-780204020795870540,\"lower\":-3972799366724268535,\"count\":1200,\"preSum\":1200},"
                + "{\"ndv\":1200,\"upper\":-248812486600746254,\"lower\":-780153738484095613,\"count\":1200,\"preSum\":2400},"
                + "{\"ndv\":1200,\"upper\":-63800360812732538,\"lower\":-248455180178683517,\"count\":1200,\"preSum\":3600},"
                + "{\"ndv\":1200,\"upper\":-14994567557673612,\"lower\":-63790889962003073,\"count\":1200,\"preSum\":4800},"
                + "{\"ndv\":1200,\"upper\":-5615906162838590,\"lower\":-14953314294517543,\"count\":1200,\"preSum\":6000},"
                + "{\"ndv\":1200,\"upper\":-953432322012743,\"lower\":-5609211735336832,\"count\":1200,\"preSum\":7200},"
                + "{\"ndv\":1200,\"upper\":-417618415533553,\"lower\":-952641436283056,\"count\":1200,\"preSum\":8400},"
                + "{\"ndv\":1200,\"upper\":-79977411919779,\"lower\":-417592132206545,\"count\":1200,\"preSum\":9600},"
                + "{\"ndv\":1200,\"upper\":-26404220982700,\"lower\":-79962379169577,\"count\":1200,\"preSum\":10800},"
                + "{\"ndv\":1200,\"upper\":-6802100335230,\"lower\":-26396267200649,\"count\":1200,\"preSum\":12000},"
                + "{\"ndv\":1200,\"upper\":-1667744197346,\"lower\":-6798228924996,\"count\":1200,\"preSum\":13200},"
                + "{\"ndv\":1200,\"upper\":-566733380709,\"lower\":-1657935937367,\"count\":1200,\"preSum\":14400},"
                + "{\"ndv\":1200,\"upper\":-96462900000,\"lower\":-565449920287,\"count\":1200,\"preSum\":15600},"
                + "{\"ndv\":1200,\"upper\":-48703543675,\"lower\":-96441135628,\"count\":1200,\"preSum\":16800},"
                + "{\"ndv\":1200,\"upper\":-8727319103,\"lower\":-48610230738,\"count\":1200,\"preSum\":18000},"
                + "{\"ndv\":1200,\"upper\":-3906619135,\"lower\":-8716148781,\"count\":1200,\"preSum\":19200},"
                + "{\"ndv\":1200,\"upper\":-772179718,\"lower\":-3901734182,\"count\":1200,\"preSum\":20400},"
                + "{\"ndv\":1200,\"upper\":-268732784,\"lower\":-771940280,\"count\":1200,\"preSum\":21600},"
                + "{\"ndv\":1200,\"upper\":-62952769,\"lower\":-268450209,\"count\":1200,\"preSum\":22800},"
                + "{\"ndv\":1200,\"upper\":-13880944,\"lower\":-62899406,\"count\":1200,\"preSum\":24000},"
                + "{\"ndv\":1200,\"upper\":-5050501,\"lower\":-13860907,\"count\":1200,\"preSum\":25200},"
                + "{\"ndv\":1198,\"upper\":-899192,\"lower\":-5049662,\"count\":1200,\"preSum\":26400},"
                + "{\"ndv\":1198,\"upper\":-399699,\"lower\":-898192,\"count\":1200,\"preSum\":27600},"
                + "{\"ndv\":1192,\"upper\":-78580,\"lower\":-399292,\"count\":1200,\"preSum\":28800},"
                + "{\"ndv\":1185,\"upper\":-27729,\"lower\":-78565,\"count\":1200,\"preSum\":30000},"
                + "{\"ndv\":1118,\"upper\":-6166,\"lower\":-27699,\"count\":1200,\"preSum\":31200},"
                + "{\"ndv\":1059,\"upper\":-983,\"lower\":-6164,\"count\":1203,\"preSum\":32400},"
                + "{\"ndv\":471,\"upper\":-465,\"lower\":-982,\"count\":1204,\"preSum\":33603},"
                + "{\"ndv\":354,\"upper\":-83,\"lower\":-464,\"count\":1222,\"preSum\":34807},"
                + "{\"ndv\":52,\"upper\":-31,\"lower\":-82,\"count\":1200,\"preSum\":36029},"
                + "{\"ndv\":25,\"upper\":8,\"lower\":-30,\"count\":1347,\"preSum\":37229},"
                + "{\"ndv\":51,\"upper\":60,\"lower\":10,\"count\":1225,\"preSum\":38576},"
                + "{\"ndv\":143,\"upper\":216,\"lower\":61,\"count\":1202,\"preSum\":39801},"
                + "{\"ndv\":492,\"upper\":754,\"lower\":217,\"count\":1201,\"preSum\":41003},"
                + "{\"ndv\":793,\"upper\":3663,\"lower\":755,\"count\":1200,\"preSum\":42204},"
                + "{\"ndv\":1052,\"upper\":8697,\"lower\":3665,\"count\":1200,\"preSum\":43404},"
                + "{\"ndv\":1150,\"upper\":47394,\"lower\":8700,\"count\":1200,\"preSum\":44604},"
                + "{\"ndv\":1190,\"upper\":99488,\"lower\":47436,\"count\":1200,\"preSum\":45804},"
                + "{\"ndv\":1200,\"upper\":588151,\"lower\":99521,\"count\":1200,\"preSum\":47004},"
                + "{\"ndv\":1200,\"upper\":1925118,\"lower\":588574,\"count\":1200,\"preSum\":48204},"
                + "{\"ndv\":1200,\"upper\":7047461,\"lower\":1937010,\"count\":1200,\"preSum\":49404},"
                + "{\"ndv\":1200,\"upper\":32039065,\"lower\":7054380,\"count\":1200,\"preSum\":50604},"
                + "{\"ndv\":1200,\"upper\":80382513,\"lower\":32071379,\"count\":1200,\"preSum\":51804},"
                + "{\"ndv\":1200,\"upper\":408805643,\"lower\":80404567,\"count\":1200,\"preSum\":53004},"
                + "{\"ndv\":1200,\"upper\":891790155,\"lower\":409596443,\"count\":1200,\"preSum\":54204},"
                + "{\"ndv\":1200,\"upper\":4905339591,\"lower\":892153441,\"count\":1200,\"preSum\":55404},"
                + "{\"ndv\":1200,\"upper\":9862596070,\"lower\":4907051699,\"count\":1200,\"preSum\":56604},"
                + "{\"ndv\":1200,\"upper\":59587348347,\"lower\":9865088435,\"count\":1200,\"preSum\":57804},"
                + "{\"ndv\":1200,\"upper\":215710772800,\"lower\":59605538090,\"count\":1200,\"preSum\":59004},"
                + "{\"ndv\":1200,\"upper\":725741398273,\"lower\":215894096412,\"count\":1200,\"preSum\":60204},"
                + "{\"ndv\":1200,\"upper\":3202379661159,\"lower\":725764367695,\"count\":1200,\"preSum\":61404},"
                + "{\"ndv\":1200,\"upper\":8304075382476,\"lower\":3202420309001,\"count\":1200,\"preSum\":62604},"
                + "{\"ndv\":1200,\"upper\":44075442676653,\"lower\":8306054672074,\"count\":1200,\"preSum\":63804},"
                + "{\"ndv\":1200,\"upper\":93700111443625,\"lower\":44081001414215,\"count\":1200,\"preSum\":65004},"
                + "{\"ndv\":1200,\"upper\":526519323012245,\"lower\":93710152841456,\"count\":1200,\"preSum\":66204},"
                + "{\"ndv\":1200,\"upper\":1589348051034905,\"lower\":528009137417938,\"count\":1200,\"preSum\":67404},"
                + "{\"ndv\":1200,\"upper\":6605493370040792,\"lower\":1593977183665555,\"count\":1200,\"preSum\":68604},"
                + "{\"ndv\":1200,\"upper\":27195135774548073,\"lower\":6613470146483421,\"count\":1200,\"preSum\":69804},"
                + "{\"ndv\":1200,\"upper\":79196366217164112,\"lower\":27222136376206135,\"count\":1200,\"preSum\":71004},"
                + "{\"ndv\":1200,\"upper\":395270570725543572,\"lower\":79212794893734578,\"count\":1200,\"preSum\":72204},"
                + "{\"ndv\":1200,\"upper\":889998013754706028,\"lower\":395490216362406003,\"count\":1200,\"preSum\":73404},"
                + "{\"ndv\":1200,\"upper\":4835437415733893260,\"lower\":890539959146931357,\"count\":1200,\"preSum\":74604},"
                + "{\"ndv\":996,\"upper\":9214668497770919103,\"lower\":4839683700842769645,\"count\":996,\"preSum\":75804}],"
                + "\"maxBucketSize\":64,\"type\":\"Int\",\"sampleRate\":0.98707926}";

        Histogram h = Histogram.deserializeFromJson(serialize);
        long lower = -9223037765499330604L;
        long upper = -75498;
        System.out.println(h.rangeCount(lower, true, upper, true));
    }

    @Test
    public void testFindBucketDateType() throws Exception {
        Arrays.stream(new DataType[] {
                DataTypes.DateType,
                DataTypes.DatetimeType,
                DataTypes.TimestampType,
                DataTypes.TimeType})
            .forEach(dt -> testTimeType(dt, mockLongData(10240, dt)));

    }

    private void testTimeType(DataType dataType, Long[] data) {
        Histogram h = new Histogram(64, dataType, 1.0F);
        h.buildFromData(data);
        Object[] testVals = new Object[] {
            data[0],
            TimeStorage.readTimestamp(data[0]).toString()
        };
        for (Object testVal : testVals) {
            long count = h.rangeCount(testVal, true, testVal, true);
            Assert.assertTrue(count > 0);
            System.out.println(count);
            String json = Histogram.serializeToJson(h);
            System.out.println(json);
            Histogram newHis = Histogram.deserializeFromJson(json);
            long newCount = newHis.rangeCount(testVal, true, testVal, true);
            System.out.println("new count:" + newCount);
            System.out.println("test val:" + testVal);
            System.out.println(Histogram.serializeToJson(newHis));
            Assert.assertTrue(DataTypeUtil.equalsSemantically(h.getDataType(), newHis.getDataType()));
            Assert.assertTrue(count == newCount);
        }

    }

    /**
     * turn mock data to long type
     */
    private Long[] mockLongData(int length, DataType dataType) {
        Long[] objArray = new Long[length];
        IntStream.range(0, length)
            .forEach(i -> objArray[i] = StatisticUtils.packDateTypeToLong(dataType, randomDate().getTime()));
        return objArray;
    }

    /**
     * mock date from 2010~2020
     */
    private Calendar randomDate() {
        Random random = new Random();
        Calendar calendar = Calendar.getInstance();

        int year = 2010 + random.nextInt(10);
        int month = random.nextInt(12);
        int maxDayOfMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        int day = 1 + random.nextInt(maxDayOfMonth);

        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        return calendar;
    }

}
