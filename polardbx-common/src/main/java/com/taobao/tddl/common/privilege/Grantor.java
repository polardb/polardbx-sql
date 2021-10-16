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

package com.taobao.tddl.common.privilege;

import com.alibaba.polardbx.common.privilege.PrivilegeUtil;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.text.MessageFormat;

public class Grantor implements Serializable {
    private static final long serialVersionUID = -798571427880241956L;

    public static final String GRANTOR_SEP = "@";

    private String user;

    private String host;

    public Grantor() {
    }

    public Grantor(String user, String host) {
        this.user = user;
        this.host = host;
    }

    @Override
    public String toString() {
        return user + GRANTOR_SEP + PrivilegeUtil.parseHost(host);
    }

    public static Grantor parse(String grantor) {
        if (StringUtils.isEmpty(grantor) || grantor.equals("\"\"")
            || grantor.equals("''")) {
            return null;
        }

        boolean legal = true;
        if (grantor == null) {
            legal = false;
        }

        String[] strs = grantor.split(GRANTOR_SEP);
        if (strs.length != 2) {
            legal = false;
        }

        if (!legal) {
            String msg =
                MessageFormat.format("Illegal grantor string: {0}! grantor format must follow user@host.", grantor);
            throw new IllegalArgumentException(msg);
        }

        Grantor record = new Grantor();
        record.setUser(strs[0]);
        record.setHost(strs[1]);
        return record;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
