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

package com.alibaba.polardbx.optimizer.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class RexLiteralTypeUtilsTest {

    @Test
    public void testBoolToLiteral() {

        try {

            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.BOOLEAN;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new Boolean(true);
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);
                Assert.assertEquals(node.getValue(), true);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            ;
            Assert.fail();
        }

    }

    @Test
    public void testTimeToLiteral() {
        try {

            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);
            {
                SqlTypeName sqlTypeName = SqlTypeName.TIME;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("20:00:00");

                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                String timeFormatStr = sdf.format(((Calendar) node.getValue()).getTime());
                Assert.assertEquals(timeFormatStr, "20:00:00");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {

            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);
            {
                SqlTypeName sqlTypeName = SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("20:00:00");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                String timeFormatStr = (String) RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node, true);

                Assert.assertEquals(timeFormatStr, "20:00:00");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDateToLiteral() {

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.DATE;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String timeFormatStr = sdf.format(((Calendar) node.getValue()).getTime());

                Assert.assertEquals(timeFormatStr, "2019-02-28");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testDateTimeToLiteral() {

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.DATETIME;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28 20:00:00");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeFormatStr = (String) RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node, true);

                Assert.assertEquals(timeFormatStr, "2019-02-28 20:00:00");

            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testTimestampToLiteral() {

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.TIMESTAMP;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28 20:00:00");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeFormatStr = sdf.format(((Calendar) node.getValue()).getTime());

                Assert.assertEquals(timeFormatStr, "2019-02-28 20:00:00");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.assertEquals(1, 2);
        }

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28 20:00:00");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeFormatStr = (String) RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node, true);

                Assert.assertEquals(timeFormatStr, "2019-02-28 20:00:00");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCharToLiteral() {

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.CHAR;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);
                Assert.assertEquals(RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node), "2019-02-28");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.assertEquals(1, 2);
        }

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.VARCHAR;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28");
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);
                Assert.assertEquals(RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node), "2019-02-28");
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testBinaryToLiteral() {

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.BINARY;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28").getBytes();
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);

                byte[] byVal = (byte[]) RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node);
                Assert.assertEquals(new String(byVal), new String("2019-02-28"));
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.assertEquals(1, 2);
        }

        try {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder builder = new RexBuilder(typeFactory);

            {
                SqlTypeName sqlTypeName = SqlTypeName.VARBINARY;
                RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
                Object obj = new String("2019-02-28").getBytes();
                RexLiteral node =
                    RexLiteralTypeUtils.convertJavaObjectToRexLiteral(obj, relDataType, sqlTypeName, builder);
                byte[] byVal = (byte[]) RexLiteralTypeUtils.getJavaObjectFromRexLiteral(node);
                Assert.assertEquals(new String(byVal), new String("2019-02-28"));
            }

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

}
