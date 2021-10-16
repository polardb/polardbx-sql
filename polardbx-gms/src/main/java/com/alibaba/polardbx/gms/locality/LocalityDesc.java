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

package com.alibaba.polardbx.gms.locality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import lombok.Value;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Descriptor of locality
 * Format:
 * single datanode: dn=dn1
 * multi datanodes: dn=[dn1,dn2]
 *
 * @author moyi
 * @since 2021/03
 */
public class LocalityDesc {

    private static final String PREFIX = "dn=";

    // list of datanode instance
    private List<String> dnList;

    public LocalityDesc() {
        this.dnList = new ArrayList<>();
    }

    public static LocalityDesc parse(String str) {
        LocalityDesc result = new LocalityDesc();

        str = StringUtils.trim(str);

        // check json format
        if (str.startsWith("{")) {
            LocalityJSON json = JSON.parseObject(str, LocalityJSON.class);
            str = json.getLocality();
        }

        if (TStringUtil.isBlank(str)) {
            return result;
        } else if (!str.startsWith(PREFIX)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "invalid locality: \"" + str + "\"");
        } else {
            str = StringUtils.trim(StringUtils.removeStart(str, PREFIX));
            if (!StringUtils.isBlank(str)) {
                List<String> dns = Arrays.asList(str.split(","));
                result.dnList = dns.stream().map(String::trim).collect(Collectors.toList());
            }
            return result;
        }
    }

    public List<String> getDnList() {
        return this.dnList;
    }

    public boolean matchStorageInstance(String storage) {
        return this.dnList == null || this.dnList.isEmpty() || this.dnList.contains(storage);
    }

    /**
     * Choose a group according to locality
     */
    public Optional<GroupDetailInfoRecord> chooseGroup(List<GroupDetailInfoRecord> groups) {
        return groups.stream()
            .sorted(Comparator.comparing(x -> x.groupName))
            .filter(x -> this.dnList.contains(x.storageInstId))
            .findFirst();
    }

    public boolean isEmpty() {
        return this.dnList == null || this.dnList.isEmpty();
    }

    @Override
    public String toString() {
        if (this.dnList == null || this.dnList.size() == 0) {
            return "";
        } else {
            return "dn=" + StringUtils.join(this.dnList, ",");
        }
    }

    public String showCreate() {
        return "/* LOCALITY='" + toString() + "' */";
    }

    /**
     * JSON serialization: {locality: 'dn=dn1'}
     */
    @Value
    static class LocalityJSON {
        public String locality;

        @JSONCreator
        public LocalityJSON(String locality) {
            this.locality = locality;
        }
    }
}
