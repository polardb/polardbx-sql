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

package com.alibaba.polardbx.repo.mysql.checktable;

import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnDiffResult {
    Boolean isDiff = true;

    List<String> diffInfos = new ArrayList<>();

    public ColumnDiffResult() {
    }

    private static String convertColumnRecordToString(ColumnsRecord columnsRecord) {
        return String.format("`%s` `%s`", columnsRecord.columnName, columnsRecord.columnType);
    }

    public static ColumnDiffResult diffPhysicalColumnAndLogicalColumnOrder(List<ColumnsRecord> physicalColumns,
                                                                           List<ColumnsRecord> logicalColumns) {
        ColumnDiffResult columnDiffResult = new ColumnDiffResult();
        physicalColumns.sort(Comparator.comparing(o -> o.ordinalPosition));
        logicalColumns.sort(Comparator.comparing(o -> o.ordinalPosition));

        List<String> columnRows1 = physicalColumns.stream().map(ColumnDiffResult::convertColumnRecordToString).collect(
            Collectors.toList());
        List<String> columnRows2 = logicalColumns.stream().map(ColumnDiffResult::convertColumnRecordToString).collect(
            Collectors.toList());

        DiffRowGenerator generator = DiffRowGenerator.create()
            .showInlineDiffs(true)
            .inlineDiffByWord(true)
            .oldTag(f -> "~~")
            .newTag(f -> "**")
            .build();
        List<DiffRow> rows = generator.generateDiffRows(
            columnRows1, columnRows2
        );
        columnDiffResult.isDiff = false;
        StringBuilder sb = new StringBuilder();
        sb.append("| physical column | logical column |\n");
        sb.append("|--------|---|\n");
        for (DiffRow diffRow : rows) {
            if (!diffRow.getTag().equals(DiffRow.Tag.EQUAL)) {
                columnDiffResult.isDiff = true;
            }
            sb.append("|").append(diffRow.getOldLine()).append("|").append(diffRow.getNewLine()).append("|\n");
        }
        columnDiffResult.diffInfos.add(sb.toString());
        return columnDiffResult;
    }

    public Boolean diff() {
        return isDiff;
    }

    public List<Object[]> convertToRows(String tableText, String opText, String status) {
        List<Object[]> results = new ArrayList<>();
        if (isDiff) {
            for (String diffInfo : diffInfos) {
                results.add(new Object[] {tableText, opText, status, diffInfo});
            }
        }
        return results;
    }
}
