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
package com.streamxhub.console.core.dao;

import com.streamxhub.console.core.entity.Application;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

/**
 * @author benjobs
 */
public interface ApplicationMapper extends BaseMapper<Application> {
    IPage<Application> page(Page<Application> page, @Param("application") Application application);

    Application getApp(@Param("application") Application application);

    void updateMonitor(@Param("application") Application application);

    @Update("update t_flink_app set deploy=#{application.deploy} where id=#{application.id}")
    void updateDeploy(@Param("application") Application application);

    @Update("update t_flink_app set state=#{application.state} where id=#{application.id}")
    void updateState(@Param("application") Application application);

    @Update("update t_flink_app set app_id=#{application.appId},job_id=#{application.jobId},state=14,end_time=null where id=#{application.id}")
    boolean mapping(@Param("application")Application paramOfApp);
}