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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.util.Collections;
import java.util.List;

/**
 * 多个simple-select语句
 *
 * @author arnkore 2016-12-27 16:38
 */
public class MultiColumnSimpleSelect extends AbstractSimpleSelect {
    private List<SingleColumnSimpleSelect> singleColumnSimpleSelects;

    protected List<String> origColumnNames;

    protected List<String> aliasColumnNames;

    public MultiColumnSimpleSelect(List<SingleColumnSimpleSelect> singleColumnSimpleSelects) {
        super(singleColumnSimpleSelects.size());
        this.singleColumnSimpleSelects = singleColumnSimpleSelects;

        origColumnNames = Lists.newArrayList();
        aliasColumnNames = Lists.newArrayList();
        for (SingleColumnSimpleSelect singleSelect : singleColumnSimpleSelects) {
            origColumnNames.add(singleSelect.getOrigColumnName());
            aliasColumnNames.add(singleSelect.getAliasColumnName());
        }
    }

    @Override
    protected void initFields(byte packetId) {
        for (int i = 0; i < singleColumnSimpleSelects.size(); i++) {
            SingleColumnSimpleSelect singleSelect = singleColumnSimpleSelects.get(i);
            if (singleSelect instanceof NonsupportedStatement) {
                GeneralUtil.nestedException("Nonsupported statement: " + singleSelect.toString());
            }

            singleSelect.initFields(packetId++);
            fields[i] = singleSelect.getFields()[0];
        }
    }

    @Override
    protected void initRowData(String resultCharset) {
        for (SingleColumnSimpleSelect singleSelect : singleColumnSimpleSelects) {
            singleSelect.initRowData(resultCharset);
            RowDataPacket singleSelectRow = singleSelect.getRow();
            row.add(singleSelectRow.fieldValues.get(0));
        }
    }

    public List<String> getOrigColumnNames() {
        return Collections.unmodifiableList(origColumnNames);
    }

    public List<String> getAliasColumnNames() {
        return Collections.unmodifiableList(aliasColumnNames);
    }
}
