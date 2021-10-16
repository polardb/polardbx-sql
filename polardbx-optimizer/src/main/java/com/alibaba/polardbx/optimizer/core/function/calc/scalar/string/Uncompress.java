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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by chuanqin on 17/12/19.
 */
public class Uncompress extends AbstractScalarFunction {
    public Uncompress(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];

        if (FunctionUtils.isNull(arg)) {
            return null;
        }

        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, arg);
        if (null == str || str.length() <= 0) {
            return str;
        }
        if (null == str || str.length() <= 0) {
            return str;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = null;
        try {
            in = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));
            GZIPInputStream gzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n = 0;
            // 将未压缩数据读入字节数组
            while ((n = gzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported encoding []", e);
        } catch (IOException e) {
            logger.error("IO Exception []", e);
        }
        throw new UnsupportedOperationException("Decompression on this column is not supported");
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UNCOMPRESS"};
    }
}
