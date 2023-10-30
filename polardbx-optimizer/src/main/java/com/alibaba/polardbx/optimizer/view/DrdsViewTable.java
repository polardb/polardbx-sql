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

package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.Util;

import java.lang.reflect.Type;
import java.util.List;

/**
 * @author dylan
 */
public class DrdsViewTable extends ViewTable {

    private SystemTableView.Row row;

    public DrdsViewTable(Type elementType, RelProtoDataType rowType, SystemTableView.Row row,
                         List<String> schemaPath, List<String> viewPath) {
        super(elementType, rowType, row.getViewDefinition(), schemaPath, viewPath);
        this.row = row;
    }

    @Override
    public RelNode toRel(
        RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
        assert context instanceof DrdsViewExpander;
        assert relOptTable instanceof RelOptTableImpl;
        return expandView((DrdsViewExpander) context, (RelOptTableImpl) relOptTable).rel;
    }

    private RelRoot expandView(DrdsViewExpander drdsViewExpander,
                               RelOptTableImpl relOptTable) {
        try {
            PlannerContext.getPlannerContext(drdsViewExpander.getCluster()).addView(
                CBOUtil.getDrdsViewTable(relOptTable).getRow().getSchemaName(),
                CBOUtil.getDrdsViewTable(relOptTable).getRow().getViewName());

            RelRoot root =
                drdsViewExpander.expandDrdsView(relOptTable.getRowType(), relOptTable, getSchemaPath(), getViewPath());

            root = root.withRel(RelOptUtil.createCastRel(root.rel, relOptTable.getRowType(), true));
            return root;
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing view definition: "
                + row.getViewDefinition(), e);
        }
    }

    public SystemTableView.Row getRow() {
        return row;
    }
}
