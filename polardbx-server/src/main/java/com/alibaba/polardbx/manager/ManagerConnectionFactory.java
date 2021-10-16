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

package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ManagerPrivileges;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.factory.FrontendConnectionFactory;

import java.nio.channels.SocketChannel;

/**
 * @author xianmao.hexm
 */
public class ManagerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        ManagerConnection c = new ManagerConnection(channel);
        c.setPrivileges(new ManagerPrivileges());
        c.setQueryHandler(new ManagerQueryHandler(c));
        c.setManaged(true);
        c.setAllowManagerLogin(CobarServer.getInstance().getConfig().getSystem().getAllowManagerLogin() != 0);
        return c;
    }

}
