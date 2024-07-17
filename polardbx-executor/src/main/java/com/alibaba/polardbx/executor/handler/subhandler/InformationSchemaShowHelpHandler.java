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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaShowHelp;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.sql.SqlKind;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author pangzhaoxing
 */
public class InformationSchemaShowHelpHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaShowHelpHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaShowHelp;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        for (String show : getShowSQL()) {
            cursor.addRow(new Object[] {show});
        }
        return cursor;
    }

    private List<String> getShowSQL() {
        List<String> shows = new ArrayList<>();
        try {
            Class clazz = Class.forName("com.alibaba.polardbx.server.parser.ServerParseShow");
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                if (field.getType() == int.class && Modifier.isStatic(field.getModifiers())) {
                    String showContent = field.getName().toLowerCase().replace("_", " ");
                    if ("other".equals(showContent)) {
                        continue;
                    }
                    shows.add("show " + showContent);
                }
            }
        } catch (ClassNotFoundException e) {
            //ignore
        }

        for (SqlKind kind : SqlKind.values()) {
            if (kind.lowerName.contains("show") && !"show".equals(kind.lowerName)) {
                String show = kind.lowerName.replace("_", " ");
                shows.add(show);
            }
        }
        return shows;
    }
}
