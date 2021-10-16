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

package com.alibaba.polardbx.rule.utils;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.rule.RuleCompatibleHelper;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import org.apache.commons.beanutils.PropertyUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenghui.lch 2017年7月12日 上午10:25:49
 * @since 5.0.0
 */
public class ShardFunctionMetaUtils {

    public static final String CLASS_URL = "classUrl";

    protected static Set<Class> metaParamTypeSet = new HashSet<Class>();

    static {
        metaParamTypeSet.add(Boolean.class);

        metaParamTypeSet.add(Short.class);
        metaParamTypeSet.add(Integer.class);
        metaParamTypeSet.add(Long.class);

        metaParamTypeSet.add(Float.class);
        metaParamTypeSet.add(Double.class);

        metaParamTypeSet.add(Number.class);

        metaParamTypeSet.add(String.class);

    }

    /**
     * 循环向上转型, 获取对象的 DeclaredField
     *
     * @param object : 子类对象
     * @return 父类中的属性对象
     */

    protected static List<Field> getDeclaredField(Object object) {
        Field field = null;

        List<Field> fieldList = new ArrayList<Field>();
        Class<?> clazz = object.getClass();

        for (; clazz != Object.class; clazz = clazz.getSuperclass()) {
            try {
                java.lang.reflect.Field[] fieldsInClass = clazz.getDeclaredFields();
                for (int i = 0; i < fieldsInClass.length; i++) {
                    fieldList.add(fieldsInClass[i]);
                }
            } catch (Exception e) {
                // 这里甚么都不要做！并且这里的异常必须这样写，不能抛出去。
                // 如果这里的异常打印或者往外抛，则就不会执行clazz =
                // clazz.getSuperclass(),最后就不会进入到父类中了
            }
        }

        return fieldList;
    }

    /**
     *
     */
    public static Map<String, Object> convertShardFuncMetaToPropertyMap(ShardFunctionMeta shardFuncMeta) {
        Map<String, Object> propertyMap = new HashMap<String, Object>();
        List<java.lang.reflect.Field> fieldsInClass = getDeclaredField(shardFuncMeta);
        for (int i = 0; i < fieldsInClass.size(); i++) {

            java.lang.reflect.Field field = fieldsInClass.get(i);
            int mod = field.getModifiers();
            if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
                // 不要静态属性和常量属性
                continue;
            }

            try {

                String fieldName = field.getName();

                Class<?> classOfField = field.getType();
                if (!metaParamTypeSet.contains(classOfField)) {
                    // 如果 规则函数的元数据的参数类型不是 JAVA的基础类型，则默认不进行xml的序列化
                    continue;
                }

                field.setAccessible(true);
                Object fieldVal = PropertyUtils.getProperty(shardFuncMeta, fieldName);
                propertyMap.put(fieldName, String.valueOf(fieldVal));

            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }

        }
        return propertyMap;
    }

    public static Object convertMapToShardFunctionMeta(Map<String, Object> shardFunctionMetaMap, Class<?> beanClass)
        throws Exception {
        if (shardFunctionMetaMap == null) {
            return null;
        }

        Object obj = beanClass.newInstance();
        List<java.lang.reflect.Field> fieldsInClass = getDeclaredField(obj);
        for (Field field : fieldsInClass) {
            int mod = field.getModifiers();
            if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
                continue;
            }
            field.setAccessible(true);
            Object fieldVal = parseObject(field, String.valueOf(shardFunctionMetaMap.get(field.getName())));
            field.set(obj, fieldVal);
        }

        return obj;
    }

    protected static Object parseObject(Field field, String objStr) {

        field.setAccessible(true);

        Object objVal = null;
        if (field.getType() == Integer.class) {
            objVal = Integer.parseInt(objStr);
        } else if (field.getType() == String.class) {
            objVal = objStr;
        } else if (field.getType() == Double.class) {
            objVal = Double.parseDouble(objStr);
        } else if (field.getType() == Float.class) {
            objVal = Float.parseFloat(objStr);
        } else if (field.getType() == Long.class) {
            objVal = Long.parseLong(objStr);
        } else if (field.getType() == Boolean.class) {
            objVal = Boolean.parseBoolean(objStr);
        } else if (field.getType() == Short.class) {
            objVal = Short.parseShort(objStr);
        }

        return objVal;
    }
}
