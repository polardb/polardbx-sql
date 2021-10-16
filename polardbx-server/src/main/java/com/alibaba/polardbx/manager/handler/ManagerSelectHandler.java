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

package com.alibaba.polardbx.manager.handler;

import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.manager.parser.ManagerParseSelect;
import com.alibaba.polardbx.manager.response.AbstractSimpleSelect;

/**
 * @author xianmao.hexm
 * @author arnkore 2016-12-27
 */
public final class ManagerSelectHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        AbstractSimpleSelect selectCommon = ManagerParseSelect.parse(stmt, offset);
        selectCommon.execute(c);
    }
}
