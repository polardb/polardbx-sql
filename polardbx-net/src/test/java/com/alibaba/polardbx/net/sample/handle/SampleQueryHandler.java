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

package com.alibaba.polardbx.net.sample.handle;

import com.alibaba.polardbx.net.handler.QueryHandler;
import com.alibaba.polardbx.net.sample.net.SampleConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public class SampleQueryHandler implements QueryHandler {

    private static final Logger logger = LoggerFactory.getLogger(SampleQueryHandler.class);

    private SampleConnection source;

    public SampleQueryHandler(SampleConnection source) {
        this.source = source;
    }

    @Override
    public void query(String sql) {
        if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder().append(source).append(sql).toString());
        }

        // sample response
        SampleResponse.response(source);
    }

}
