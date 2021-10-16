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

package com.alibaba.polardbx.atom;

import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.SQLException;

public interface TAtomDsStandard extends DataSource, Lifecycle {

    @Deprecated
    void init(String appName, String dsKey, String unitName);

    void init(String appName, String groupKey, String dsKey, String unitName);

    void init(String appName, String groupKey, String dsKey, String unitName, TAtomDsConfDO atomDsConf);

    @Override
    void setLogWriter(PrintWriter out) throws SQLException;

    @Override
    void setLoginTimeout(int seconds) throws SQLException;

    TAtomDbStatusEnum getDbStatus();

    void destroyDataSource() throws Exception;

}
