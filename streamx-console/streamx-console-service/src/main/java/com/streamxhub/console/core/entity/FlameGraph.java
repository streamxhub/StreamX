/**
 * Copyright (c) 2019 The StreamX Project
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
package com.streamxhub.console.core.entity;


import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamxhub.common.util.DeflaterUtils;
import com.wuwenze.poi.annotation.Excel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author benjobs
 */
@Data
@TableName("t_flame_graph")
@Excel("火焰图")
@Slf4j
public class FlameGraph {

    private Long id;

    private Long appId;

    private String profiler;

    private Date timeline;

    private String content;

    private transient Date end = new Date();

    private transient Integer duration = 60;

    private transient Integer width = 1280;

    private final transient Integer QUERY_DURATION = 60 * 4;

    @JsonIgnore
    public Date getStart() {
        if (this.duration > QUERY_DURATION) {
            throw new IllegalArgumentException("[StreamX] flameGraph query duration cannot be greater than 4 hours");
        }
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getDefault());
        cal.setTime(this.getEnd());
        cal.add(Calendar.MINUTE, 0 - duration);
        return cal.getTime();
    }

    @JsonIgnore
    public String getUnzipContent() {
        if (this.content != null) {
            return DeflaterUtils.unzipString(this.content);
        }
        return null;
    }

}
