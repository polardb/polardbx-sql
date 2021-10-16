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

package com.alibaba.polardbx.net.handler;

import java.nio.charset.Charset;

/**
 * 查询处理器
 *
 * @author xianmao.hexm
 */
public interface QueryHandler {

    default void queryRaw(byte[] data, int offset, int length, Charset charset) {
        query(new String(data, offset, length, charset));
    }

    void query(String sql);
}
