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

package com.alibaba.polardbx.optimizer.core.function.calc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.AbstractDataType;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;

import java.util.List;

@SuppressWarnings("rawtypes")
public abstract class UserDefinedJavaFunction extends AbstractScalarFunction {
  protected static final Logger logger = LoggerFactory.getLogger(UserDefinedJavaFunction.class);
  protected List<DataType> userInputType;
  protected DataType userResultType;

  protected UserDefinedJavaFunction(List<DataType> operandTypes, DataType resultType) {
    super(operandTypes, resultType);
  }

  @Override
  public Object compute(Object[] args, ExecutionContext ec) {
    if (args.length != userInputType.size()) {
      throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Parameters do not match input types");
    }

    //对入参进行处理
    for (int i = 0; i < args.length; i++) {
      DataType type = userInputType.get(i);

      if (type instanceof VarcharType || type instanceof CharType) {
        args[i] = DataTypes.StringType.convertFrom(args[i]);
        continue;
      }

      args[i] = type.convertFrom(args[i]);
    }
    return resultType.convertFrom(compute(args));
  }

  //用户复写方法
  public abstract Object compute(Object[] input);

  public void setUserInputType(List<DataType> userInputType) {
    this.userInputType = userInputType;
  }

  public void setUserResultType(DataType userResultType) {
    this.userResultType = userResultType;
  }
}
