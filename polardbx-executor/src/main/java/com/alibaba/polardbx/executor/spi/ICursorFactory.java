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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

/**
 * 这个接口的作用，就是用来进行各种基于cursor的转换处理的。 比如，如果sql中出现了alias
 * 那么这里就会有个aliasCursor做对应转换关系的这个逻辑。
 * 这层接口的作用在于，在优化时，可以复写这些实现，从而能够做到可以按照自己的存储特点，对特定查询进行优化的目的。
 *
 * @author whisper
 */
public interface ICursorFactory {

    Cursor repoCursor(ExecutionContext executionContext, RelNode plan);

}
