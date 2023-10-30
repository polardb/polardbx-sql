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

package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * The meta interface of desc a partition function
 * (including built-in functions and user-defined functions)
 *
 * @author chenghui.lch
 */
public interface PartitionFunctionMeta {

    /**
     * Get the implementation of partition function
     */
    PartitionIntFunction getPartitionFunction();

    /**
     * Get the sql node of partition function
     */
    SqlOperator getPartitionFunctionAst();

    /**
     * return the datatypes the of input columns
     */
    List<DataType> getInputDataTypes();

    /**
     * return all positions of the partition columns of all input of java func
     */
    List<Integer> getPartColInputPositions();

    /**
     * return all positions of the conster expr of all input of java func
     */
    List<Integer> getConstExprInputPositions();

    /**
     * return the datatype of output data
     */
    DataType getOutputDataType();

    /**
     *
     */
    List<PartitionField> getConstExprParamsFields();

    /**
     * Clone a partition function meta
     */
    Object cloneMeta();

    /**
     * calcate the hashcode of meta
     */
    int calcateHashCode();

    /**
     * equals compare with other object
     */
    boolean equalsCompare(Object obj);

    /**
     * to string
     */
    String toStringContent();

}
