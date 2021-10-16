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

package com.alibaba.polardbx.optimizer.core.dialect;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Created by lingce.ldm on 2016/12/5.
 */
public abstract class Dialect implements DialectInterface {

    public static final Logger logger = LoggerFactory.getLogger(Dialect.class);

    public final String quoteIdentifier(String identifier) {
        StringBuilder buf = new StringBuilder();
        buf.append(getIdentifierQuoteString()).append(identifier).append(getIdentifierQuoteString());
        return buf.toString();
    }

    public final String removeQuoteIdentifier(String identifier) {
        String quoteString = getIdentifierQuoteString();
        if (identifier != null && identifier.startsWith(quoteString) && identifier.endsWith(quoteString)) {
            identifier = identifier.substring(quoteString.length(), identifier.length() - quoteString.length());
        }
        return identifier;
    }

    public abstract SqlTypeName getSqlTypeName(String typeName);

    public long getDecimalPrecision(long precision) {
        return precision;
    }
}
