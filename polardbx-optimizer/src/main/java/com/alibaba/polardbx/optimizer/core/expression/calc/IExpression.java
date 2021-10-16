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

package com.alibaba.polardbx.optimizer.core.expression.calc;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by chuanqin on 17/7/13.
 */
public interface IExpression {
    Object eval(Row row);

    Object eval(Row row, ExecutionContext ec);

    /**
     * Eval the func val of FUNC(func_arg) and convert interval space
     * <pre>
     *
     *  When it is doing partition pruning,
     *
     *  this func will eval the func val of FUNC(func_arg) and
     *
     *   convert interval space from the field interval space
     *       "col cmp const"
     *   into  func interval space
     *       "func(col) cmp2 func(const)"
     *
     *   SYNOPSIS
     *       cmpDirection (input params)
     *               FALSE  <=> The expect interval is "x < const" or "x <= const"
     *               TRUE   <=> The expect interval is "x > const" or "x >= const"
     *
     *       includeEqual (output params)
     *               IN
     *                   FALSE <=> the comparison of cmp is not included the endPoint, that means:
     *                       if cmpDirection=false, the expect interval is  x < const;
     *                       if cmpDirection=true, the expect interval is  x > const;
     *
     *                   TRUE  <=> the comparison of cmp is included the endPoint, that means:
     *                       if cmpDirection=false, the expect interval is  x <= const;
     *                       if cmpDirection=true, the expect interval is  x >= const;
     *
     *              OUT
     *                  The same but for the "F(x) cmp2 F(const)" comparison, that means
     *                  the out val is converted to includeEqual of func(x).
     *
     *  DESCRIPTION
     *    This function is defined only for unary monotonic functions. The caller
     *    supplies the source half-interval
     *
     *       x com const
     *
     *    The value of const is supplied implicitly as the value this item's
     *    argument, the form of cmp comparison is specified through the
     *    function's arguments. The caller returns the result interval
     *       F(x) comp2 F(const)
     *    passing back F(const) as the return value, and the form of cmp2
     *    through the out parameter.
     *    NULL values are assumed to be comparable and be less than any non-NULL values.
     *
     *  RETURN
     *    The output range bound, which equal to the value of eval()
     *     - If the value of the function is NULL then the bound is the
     *        smallest possible value of LLONG_MIN
     * </pre>
     *
     * @param cmpDirection false: "<" 或 "<=", true: ">" 或 ">="
     * @param inclEndp true: endpoint included, false: endpoint not included
     */
    Object evalEndPoint(Row row, ExecutionContext ec, Boolean cmpDirection, AtomicBoolean inclEndp);
}
