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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimizer hint
 *
 * @author hongxi.chx
 */
public class OptimizerHint extends SqlNodeList {

    public final static String COMMIT_ON_SUCCESS = "COMMIT_ON_SUCCESS";
    public final static String ROLLBACK_ON_FAIL = "ROLLBACK_ON_FAIL";
    public final static String TARGET_AFFECT_ROW = "TARGET_AFFECT_ROW";

    private List<String> hints = new ArrayList<>();

    /**
     * Creates a node.
     */
    public OptimizerHint() {
        super(SqlParserPos.ZERO);
    }

    public List<String> getHints() {
        return hints;
    }

    public void addHint(String hint) {
        hints.add(hint);
    }

    public boolean containsHint(String hint) {
        for (String str : hints) {
            if (StringUtils.isNotEmpty(str) && (str.contains(hint.toUpperCase()))) {
                return true;
            }
        }
        return false;
    }

    public boolean containsInventoryHint() {
        return containsHint(COMMIT_ON_SUCCESS) || containsHint(ROLLBACK_ON_FAIL)
            || containsHint(TARGET_AFFECT_ROW);
    }

    @Override
    public OptimizerHint clone(SqlParserPos pos) {
        final OptimizerHint optemizerHint = new OptimizerHint();
        optemizerHint.hints = this.hints;
        return optemizerHint;
    }

    @Override
    public String toString() {
        return "OptemizerHint{" +
            hints +
            '}';
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        StringBuilder hintBuilder = new StringBuilder();
        for (int i = 0; i < hints.size(); i++) {
            final String hint = hints.get(i);
            if (StringUtils.isEmpty(hint)) {
                continue;
            }
            if (hintBuilder.length() == 0) {
                hintBuilder.append(' ');
            }
            hintBuilder.append("/*").append(hint).append("*/");
            if (hints.size() - 1 == i) {
                hintBuilder.append(' ');
            }
        }
        if (hintBuilder.length() > 0) {
            writer.print(hintBuilder.toString());
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof OptimizerHint)) {
            return litmus.fail("{} != {}", this, node);
        }
        OptimizerHint that = (OptimizerHint) node;
        if (this.size() != that.size()) {
            return litmus.fail("{} != {}", this, node);
        }
        for (int i = 0; i < hints.size(); i++) {
            String thisChild = hints.get(i);
            final String thatChild = that.hints.get(i);
            if (!thisChild.equals(thatChild)) {
                return litmus.fail(null);
            }
        }
        return litmus.succeed();
    }
}
