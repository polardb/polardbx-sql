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

package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

/**
 * 描述一个列信息，可能会是字段列，函数列，常量列<br>
 * 使用RT泛型解决子类流式API需要的返回结果为子类
 *
 * @since 5.0.0
 */
public interface ISelectable<RT extends ISelectable> extends Comparable {

    public DataType getDataType();

    // --------------- name相关信息 ----------------------

    /**
     * alias ，只在一个表（索引）对象结束的时候去处理 一般情况下，取值不需要使用这个东西。 直接使用columnName即可。
     */
    public String getAlias();

    public String getTableName();

    public String getColumnName();

    public RT setAlias(String alias);

    public RT setTableName(String tableName);

    public RT setColumnName(String columnName);

    // -----------------对象复制 -----------------

    public RT copy();

    public RT setField(Field field);

    public Field getField();

}
