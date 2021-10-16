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

package com.alibaba.polardbx.optimizer.hint.operator;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

/**
 * @author chenmo.cm
 * @date 2018/2/28 下午3:12
 */
public interface HintPushOperator extends HintOperator {

    /**
     * Replace specific part in origin SqlNode with gaven sql partial
     * @param origin origin SqlNode
     * @return
     */
    SqlNode handle(SqlSelect origin);
}
