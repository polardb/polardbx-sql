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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.encrypt.aes.AesConst;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.core.row.RowWrapper;
import com.alibaba.polardbx.optimizer.exception.FunctionException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * 一些相同的 IFunction 处理函数合并到公共类。
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 */
public class FunctionUtils {

    public static boolean isNull(Object o) {
        if (o instanceof com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row.RowValue) {
            return allElementsNull(
                ((com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row.RowValue) o).getValues());
        }
        if (o == null) {
            return true;
        }
        if (o instanceof NullValue) {
            return true;
        }
        return false;
    }

    public static boolean allElementsNull(List args) {
        boolean allArgsNull = true;
        for (Object arg : args) {
            allArgsNull = allArgsNull && isNull(arg);
        }
        return allArgsNull;
    }

    public static Row fromIRowSetToArrayRowSet(Row rsrs) {
        // 初衷是为了让mysql的代理换成实际数据
        if (rsrs == null) {
            return null;
        }
        if (rsrs instanceof ResultSetRow || rsrs instanceof JoinRow) {
            List<Object> values = rsrs.getValues();
            Row rs = new ArrayRow(rsrs.getParentCursorMeta(), values.toArray(), rsrs.estimateSize());
            return rs;
        } else if (rsrs instanceof RowWrapper) {
            return IRowSetWrapperToArrayRowSet((RowWrapper) rsrs);
        } else {
            return rsrs;
        }
    }

    /**
     * 将Wrapper实际包含的rowset中的代理去掉
     */
    private static Row IRowSetWrapperToArrayRowSet(RowWrapper rsrs) {
        if (rsrs == null) {
            return null;
        }

        Row irs = rsrs.getParentRowSet();
        if (irs instanceof ResultSetRow) {
            List<Object> values = rsrs.getValues();
            Row rs = new ArrayRow(irs.getParentCursorMeta(), values.toArray(), irs.estimateSize());
            rsrs.setParentRowSet(rs);
        } else if (irs instanceof RowWrapper) {
            IRowSetWrapperToArrayRowSet((RowWrapper) irs);
        }

        return rsrs;
    }

    public static boolean convertConditionToBoolean(Object condition) {
        if (condition instanceof Boolean) {
            return (boolean) condition;
        } else if (condition instanceof Number) {
            return ((BigInteger)DataTypes.ULongType.convertJavaFrom(condition)).compareTo(BigInteger.ZERO) != 0;
        } else if (condition == null) {
            return false;
        } else {
            GeneralUtil.nestedException("Illegal condition value: " + condition.getClass());
        }
        return false;
    }

    public static boolean atLeastOneElementsNull(List<Object> args) {
        for (Object arg : args) {
            if (isNull(arg)) {
                return true;
            }
        }
        return false;
    }

    public static boolean atLeastOneElementsNull(Object[] args) {
        return atLeastOneElementsNull(Arrays.asList(args));
    }

    /**
     * For now([fsp]) or other time functions using fsp parameter.
     *
     * @param args parameters of function.
     */
    public static void checkFsp(Object[] args) {
        if (args != null && args.length > 0) {
            int fsp = DataTypes.IntegerType.convertFrom(args[0]);
            if (fsp > 6) {
                throw new FunctionException("Too big precision " + fsp
                    + " specified for column 'now'. Maximum is 6.");
            } else if (fsp < 0) {
                throw new FunctionException("Too small precision " + fsp
                    + " specified for column 'now'. Minimum is 0.");
            }
        }
    }

    /**
     * 对初始向量进行校验并转换
     * 若位数不足则报错
     */
    public static byte[] parseInitVector(Object arg, DataType dataType, String funcName) {
        if (FunctionUtils.isNull(arg)) {
            throw new FunctionException(
                "Incorrect parameter count in the call to native function '"
                    + funcName + "'");
        }
        byte[] initVector = DataTypeUtil.convert(dataType, DataTypes.BytesType, arg);
        if (initVector.length < AesConst.IV_LENGTH) {
            throw new FunctionException(
                "The initialization vector supplied to " + funcName + " is too short. Must be at least 16 bytes long");
        }
        return initVector;
    }
}
