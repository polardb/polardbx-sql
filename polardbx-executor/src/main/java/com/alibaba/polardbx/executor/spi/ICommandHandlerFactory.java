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

package com.alibaba.polardbx.executor.spi;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

/**
 * <pre>
 * 用来进行具体的，某一个Executor的，构造方法.
 * 如果需要从执行节点层复写存储执行的过程，可以对这个接口进行修改。
 * 比如mysql需要将原来的更新逻辑做合并。ex update set a = 1 where id = 1;
 * 在传递中，使用的方式是一个updateNode,带一个QueryNode
 * 这种查询模式，无法简单的直接被转换为一个update sql,而只能转变为一个update id in.和一个select* from tab where id =1 这样的查询效率太低。
 * 因此，可以直接从updateNode这里，就将updateNode和selectNode在这个地方直接变成sql去执行。而不走到cursor层合并s能够简化查询编码。
 * </pre>
 *
 * @author whisper
 */
public interface ICommandHandlerFactory {
    PlanHandler getCommandHandler(RelNode logicalPlan, ExecutionContext executionContext);
}
