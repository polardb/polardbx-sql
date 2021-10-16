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

package com.alibaba.polardbx.net.sample.net;

import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.factory.FrontendConnectionFactory;
import com.alibaba.polardbx.net.sample.SamplePrivileges;
import com.alibaba.polardbx.net.sample.handle.SampleQueryHandler;

import java.nio.channels.SocketChannel;

/**
 * @author xianmao.hexm 2011-4-19 下午02:53:00
 */
public class SampleConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        SampleConnection c = new SampleConnection(channel);
        c.setPrivileges(new SamplePrivileges());
        c.setQueryHandler(new SampleQueryHandler(c));
        return c;
    }

}
