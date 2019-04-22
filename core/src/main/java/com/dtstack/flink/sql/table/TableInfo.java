/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 

package com.dtstack.flink.sql.table;

import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * Reason: 基础表的数据结构
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class TableInfo {

    public static final String PARALLELISM_KEY = "parallelism";

    private String name;

    // 这是表类型,es 就是  elasticsearch
    private String type;

    private String[] fields;

    private String[] fieldTypes;

    private Class<?>[] fieldClasses;

    // 包含所有字段的列表
    private final List<String> fieldList = Lists.newArrayList();

    private final List<String> fieldTypeList = Lists.newArrayList();

    private final List<Class> fieldClassList = Lists.newArrayList();

    private List<String> primaryKeys;

    private Integer parallelism = 1;

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public abstract boolean check();

    public String getType() {
        return type;
    }

    // 设置表类型
    public void setType(String type) {
        this.type = type;
    }

    public String[] getFields() {
        return fields;
    }

    public Class<?>[] getFieldClasses() {
        return fieldClasses;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        if(parallelism == null){
            return;
        }

        if(parallelism <= 0){
            throw new RuntimeException("Abnormal parameter settings: parallelism > 0");
        }

        this.parallelism = parallelism;
    }

    public void addField(String fieldName){
        fieldList.add(fieldName);
    }

    public void addFieldClass(Class fieldClass){
        fieldClassList.add(fieldClass);
    }

    public void addFieldType(String fieldType){
        fieldTypeList.add(fieldType);
    }


    public void finish(){
        // 将字段列表转到字段数组
        this.fields = fieldList.toArray(new String[fieldList.size()]);
        this.fieldClasses = fieldClassList.toArray(new Class[fieldClassList.size()]);
        this.fieldTypes = fieldTypeList.toArray(new String[fieldTypeList.size()]);
    }
}
