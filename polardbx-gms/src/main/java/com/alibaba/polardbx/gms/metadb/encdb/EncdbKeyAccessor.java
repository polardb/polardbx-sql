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

package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncdbKeyAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncdbKeyAccessor.class);

    public static final String ENCDB_KEY = wrap(GmsSystemTables.ENCDB_KEY);

    private static final String COLUMNS = "`id`, `key`, `type`";

    private static final String VALUES = "null,?,?";

    private static final String INSERT_KEY = "insert into " + ENCDB_KEY + "(" + COLUMNS + ") values (" + VALUES + ")";

    private static final String REPLACE_KEY = "replace into " + ENCDB_KEY + "(" + COLUMNS + ") values (" + VALUES + ")";

    private static final String SELECT_ALL_KEY = "select " + COLUMNS + " from " + ENCDB_KEY;

    private static final String SELECT_KEY =
        "select " + COLUMNS + " from " + ENCDB_KEY + " where type=?";

    private static final String DELETE_ALL_KEY = "delete from " + ENCDB_KEY + " where `type` = ?";

    private static final String DELETE_BY_KEY = "delete from " + ENCDB_KEY + " where `type` = ? and `key_hash` = ?";

    public int insertMekHash(String keyHash) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, keyHash);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, EncdbKey.KeyType.MEK_HASH.name());
        return insert(INSERT_KEY, ENCDB_KEY, params);
    }

    public int replaceMekHash(String keyHash) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, keyHash);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, EncdbKey.KeyType.MEK_HASH.name());
        return insert(REPLACE_KEY, ENCDB_KEY, params);
    }

    public EncdbKey getMekHash() {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, EncdbKey.KeyType.MEK_HASH.name());
        List<EncdbKey> keyList = query(SELECT_KEY, ENCDB_KEY, EncdbKey.class, params);
        return keyList.isEmpty() ? null : keyList.get(0);
    }

    public int insertMekEnc(String keyEnc) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, keyEnc);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, EncdbKey.KeyType.MEK_ENC.name());
        return insert(INSERT_KEY, ENCDB_KEY, params);
    }

    public EncdbKey getMekEnc() {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, EncdbKey.KeyType.MEK_ENC.name());
        List<EncdbKey> keyList = query(SELECT_KEY, ENCDB_KEY, EncdbKey.class, params);
        return keyList.isEmpty() ? null : keyList.get(0);
    }

}
