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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class MockViewManager extends ViewManager {

    // viewName -> (column, viewDefinition)
    private Map<String, Pair<List<String>, String>> mockSchemaViews;

    private static final MockViewManager INSTANCE;

    static {
        INSTANCE = new MockViewManager();
        INSTANCE.init();
    }

    private MockViewManager() {
        super(null, null, null);
    }

    public static MockViewManager getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        mockSchemaViews = TreeMaps.caseInsensitiveMap();
        definePolarXView();
    }

    @Override
    protected void doDestroy() {
        // pass
    }

    @Override
    public SystemTableView.Row select(String viewName) {
        viewName = viewName.toLowerCase();
        Pair<List<String>, String> pair = mockSchemaViews.get(viewName);
        if (pair != null) {
            return new SystemTableView.Row(InformationSchema.NAME, viewName, pair.getKey(), pair.getValue());
        } else {
            return null;
        }
    }

    @Override
    public void invalidate(String viewName) {
        throw new AssertionError();
    }

    @Override
    public boolean insert(String viewName, List<String> columnList, String viewDefinition, String definer,
                          String planString, String planType) {
        throw new AssertionError();
    }

    @Override
    public boolean replace(String viewName, List<String> columnList, String viewDefinition, String definer,
                           String planString, String planType) {
        throw new AssertionError();
    }

    @Override
    public boolean delete(String viewName) {
        throw new AssertionError();
    }

    private void defineView(String name, String[] columns, String definition) {
        mockSchemaViews.put(name,
            Pair.of(columns == null ? null : Arrays.stream(columns).collect(Collectors.toList()), definition));
    }

    private void definePolarXView() {
        defineCommonView();
    }

    private void defineCommonView() {

    }
}

