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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    //TODO: locality="dn=dn1,dn2;balance_single_table=on/off;storage_group=g1;"
    private static final String DN_PREFIX = "dn=";

    private static final String BALANCE_PREFIX = "balance_single_table=";

    // list of datanode instance
    private List<String> dnList;

    private Boolean balanceSingleTable;

    public LocalityDesc() {
        this.dnList = new ArrayList<>();
        this.balanceSingleTable = false;
    }

    public LocalityDesc(List<String> dnList) {
        this.dnList = dnList.stream().sorted().collect(Collectors.toList());
        this.balanceSingleTable = false;
    }

    public static LocalityDesc parse(String str) {
        LocalityDesc result = new LocalityDesc();
        if (str == null) {
            return result;
        }
        str = StringUtils.trim(str);

        // json
        if (str.startsWith("{")) {
            LocalityJSON json = JSON.parseObject(str, LocalityJSON.class);
            str = json.getLocality();
        }

        if (TStringUtil.isBlank(str)) {
            return result;
        }

        List<String> options = Arrays.asList(str.split(";"));
        for (int i = 0; i < options.size(); i++) {
            String option = options.get(i);
            if (option.startsWith(DN_PREFIX)) {
                String dnString = StringUtils.trim(StringUtils.removeStart(option, DN_PREFIX));
                if (!dnString.isEmpty()) {
                    String[] dns = StringUtils.trim(StringUtils.removeStart(option, DN_PREFIX)).split(",");
                    result.dnList =
                        Arrays.asList(dns).stream().map(String::trim).sorted().distinct().collect(Collectors.toList());
                }
            } else if (option.startsWith(BALANCE_PREFIX)) {
                Map<String, Boolean> flagMap = new HashMap<String, Boolean>() {{
                    put("true", true);
                    put("false", false);
                    put("on", true);
                    put("off", false);
                }};
                String balanceFlag = StringUtils.trim(StringUtils.removeStart(option, BALANCE_PREFIX));
                if (flagMap.containsKey(balanceFlag)) {
                    result.balanceSingleTable = flagMap.get(balanceFlag);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format(
                        "invalid locality: '%s', balance_single_table option value illegal '%s', must be 'true' or 'false'",
                        str, balanceFlag));
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format("invalid locality: '%s', must start with '%s' or '%s' or be empty string.",
                        str, DN_PREFIX, BALANCE_PREFIX));
            }
        }
        return result;
    }

    public List<String> getDnList() {
        return this.dnList;
    }

    public Boolean getBalanceSingleTable() {
        return balanceSingleTable;
    }

    //localityDesc 1 >= localityDesc 2
    public boolean compactiableWith(LocalityDesc localityDesc) {
        if (dnList == null || dnList.isEmpty()) {
            return true;
        } else if (localityDesc.holdEmptyDnList()) {
            return true;
        } else {
            return dnList.containsAll(localityDesc.getDnList());
        }
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

    public boolean holdEmptyDnList() {
        return this.dnList == null || this.dnList.isEmpty();
    }

    public String getDnString() {
        String result = "";
        if (!this.holdEmptyDnList()) {
            result += "dn=" + StringUtils.join(this.dnList, ",");
        }
        return result;
    }

    public Boolean match(LocalityDesc localityDesc) {
        return this.getDnString().equals(localityDesc.getDnString());
    }

    @Override
    public String toString() {
        String result = "";
        List<String> options = new ArrayList<>();
        if (this.balanceSingleTable) {
            options.add("balance_single_table=on");
        }
        if (!this.holdEmptyDnList()) {
            options.add("dn=" + StringUtils.join(this.dnList, ","));
        }
        if (!options.isEmpty()) {
            result = StringUtils.join(options, ",");
        }
        return result;
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
