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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lijun.cailj 2018/8/13
 */
public class MysqlCreateFullTextAnalyzerStatement extends MySqlStatementImpl {


    private SQLName    name;

    private String tokenizer;
    private List<String> charfilters = new ArrayList<String>();
    private List<String> tokenizers = new ArrayList<String>();

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public String getTokenizer() {
        return tokenizer;
    }

    public void setTokenizer(String tokenizer) {
        this.tokenizer = tokenizer;
    }

    public List<String> getCharfilters() {
        return charfilters;
    }

    public List<String> getTokenizers() {
        return tokenizers;
    }

}
