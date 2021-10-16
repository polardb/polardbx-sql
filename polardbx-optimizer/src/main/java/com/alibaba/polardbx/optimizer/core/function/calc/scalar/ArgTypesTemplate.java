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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar;

import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;

/**
 * Created by chuanqin on 2019/8/14.
 */
public class ArgTypesTemplate {

    private static RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private static RelDataType integerType = factory.createSqlType(SqlTypeName.INTEGER);

    private static RelDataType bigIntegerType = factory.createSqlType(SqlTypeName.BIGINT_UNSIGNED);

    private static RelDataType stringType = factory.createSqlType(SqlTypeName.VARCHAR);

    private static RelDataType charType = factory.createSqlType(SqlTypeName.CHAR);

    private static RelDataType bytesType = factory.createSqlType(SqlTypeName.VARBINARY);

    public static List<RelDataType> INT_INT = Arrays.asList(integerType, integerType);

    public static List<RelDataType> BIGINT_BIGINT = Arrays.asList(bigIntegerType, bigIntegerType);

    public static List<RelDataType> STRING_BYTES = Arrays.asList(stringType, bytesType);

    public static List<RelDataType> BYTES_STRING = Arrays.asList(bytesType, stringType);

    public static List<RelDataType> BYTES_CHAR = Arrays.asList(bytesType, charType);

    public static List<RelDataType> CHAR_BYTES = Arrays.asList(charType, bytesType);

}
