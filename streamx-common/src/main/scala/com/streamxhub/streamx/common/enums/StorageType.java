/*
 * Copyright (c) 2021 The StreamX Project
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.streamxhub.streamx.common.enums;

import org.apache.commons.lang3.StringUtils;

public enum StorageType {

    HDFS("hdfs"),
    LFS("lfs");

    private final String identifier;

    StorageType(String identifier) {
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    public static StorageType of(String identifier) {
        if (StringUtils.isEmpty(identifier)) {
            return LFS;
        }
        for (StorageType type : values()) {
            if (type.identifier.equals(identifier)) {
                return type;
            }
        }
        return LFS;
    }

}
