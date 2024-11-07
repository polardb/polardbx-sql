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

package com.alibaba.polardbx.optimizer.hint.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.hint.SimpleHintParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class HintCmdCoronaDbJson extends BaseHintOperator implements HintCmdOperator {

    private final String json;
    private final String type;
    private final String realtabs;

    public HintCmdCoronaDbJson(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);

        String tmpJson = "";

        for (HintArgKey arg : getArgKeys()) {
            SqlNode value = this.argMap.get(arg);

            if (null == value) {
                continue;
            }

            switch (arg.ordinal) {
            case 0:
                tmpJson = RelUtils.stringValue(value);
                break;
            default:
                break;
            } // end of switch
        } // end of for

        this.json = tmpJson;
        if (TStringUtil.isNotEmpty(json)) {
            JSONObject parsed = JSON.parseObject(json);
            this.type = parsed.getString(SimpleHintParser.TYPE);
            this.realtabs = parsed.getString(SimpleHintParser.REALTABS);
        } else {
            this.type = null;
            this.realtabs = null;
        }
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.CORONADB_JSON_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        if (TStringUtil.isNotBlank(this.json)) {
            current.setJson(this.json);
        }
        return current;
    }

    @Override
    public String oldStyle() {
        StringBuilder result = new StringBuilder("/*+TDDL(");
        result.append(json);
        result.append(")*/");

        return result.toString();
    }

    public boolean isDirect() {
        return null != type && TStringUtil.equalsIgnoreCase(type, SimpleHintParser.TYPE_DIRECT)
            && TStringUtil.isBlank(realtabs);
    }

    public boolean isDirectWithRealTableName() {
        return null != type && TStringUtil.equalsIgnoreCase(type, SimpleHintParser.TYPE_DIRECT)
            && !TStringUtil.isBlank(realtabs);
    }

    public boolean isUseRouteCondition() {
        return null != type && !type.isEmpty();
    }
}
