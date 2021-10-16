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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FastsqlUtils {

    private static final List<SQLParserFeature> DEFAULT_FEATURES = Arrays.asList(
        SQLParserFeature.TDDLHint,
        SQLParserFeature.EnableCurrentUserExpr,
        SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DRDSBaseline,
        SQLParserFeature.DrdsGSI,
        SQLParserFeature.DrdsMisc,
        SQLParserFeature.DrdsCCL
    );

    public static List<SQLStatement> parseSql(String stmt, SQLParserFeature... features) {
        List<SQLParserFeature> featuresList = new ArrayList<>(features.length + DEFAULT_FEATURES.size());
        featuresList.addAll(Arrays.asList(features));
        featuresList.addAll(DEFAULT_FEATURES);
        MySqlStatementParser parser = new MySqlStatementParser(stmt, featuresList.toArray(new SQLParserFeature[0]));
        return parser.parseStatementList();
    }

    public static List<SQLStatement> parseSql(ByteString stmt, SQLParserFeature... features) {
        List<SQLParserFeature> featuresList = new ArrayList<>(features.length + DEFAULT_FEATURES.size());
        featuresList.addAll(Arrays.asList(features));
        featuresList.addAll(DEFAULT_FEATURES);
        MySqlStatementParser parser = new MySqlStatementParser(stmt, featuresList.toArray(new SQLParserFeature[0]));
        return parser.parseStatementList();
    }

}
