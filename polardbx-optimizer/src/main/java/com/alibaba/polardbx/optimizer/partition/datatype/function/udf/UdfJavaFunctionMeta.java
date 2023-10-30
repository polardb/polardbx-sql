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

package com.alibaba.polardbx.optimizer.partition.datatype.function.udf;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * The meta info of a user defined java function
 *
 * @author chenghui.lch
 */
public interface UdfJavaFunctionMeta {

    /**
     * Get the user-defined java function of scalar function
     */
    UserDefinedJavaFunction getUdfJavaFunction();

    /**
     * Get the user-defined java function of sql node
     */
    SqlOperator getUdfJavaFunctionAst();

    /**
     * return the datatypes the of input columns
     */
    List<DataType> getInputDataTypes();

    /**
     * return the datatyps of output data
     */
    DataType getOutputDataType();

}
