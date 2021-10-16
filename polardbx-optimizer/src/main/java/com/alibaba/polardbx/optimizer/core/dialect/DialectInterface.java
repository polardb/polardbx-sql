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

package com.alibaba.polardbx.optimizer.core.dialect;

import org.apache.calcite.sql.SqlDialect;

/**
 * Created by lingce.ldm on 2016/12/5.
 */
public interface DialectInterface {
    /**
     * 获取对应的Calcite的SqlDialect
     */
    SqlDialect getCalciteSqlDialect();

    /**
     * 获取驱动名
     */
    String getDriverName();

    /**
     * 获取标识符的引用字符串
     */
    String getIdentifierQuoteString();

    /**
     * 获取验证该数据库系统的SQL语句
     */
    String getValidationSql();
}
