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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.beanutils.PropertyUtils;

import java.lang.reflect.InvocationTargetException;

public class PropertyReflectUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertyReflectUtil.class);

    public static Object getPropertyValue(Object bean, String name) {
        try {
            return PropertyUtils.getProperty(bean, name);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Illegal property column!");
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Illegal property column!");
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Illegal property column!");
        }
    }

    public static void setPropertyValue(Object bean, String propertyName, Object val) {
        try {
            PropertyUtils.setProperty(bean, propertyName, val);
        } catch (IllegalAccessException e) {
            logger.error("Should not happen here!");
        } catch (InvocationTargetException e) {
            logger.error("Should not happen here!");
        } catch (NoSuchMethodException e) {
            logger.error("Should not happen here!");
        }
    }
}
