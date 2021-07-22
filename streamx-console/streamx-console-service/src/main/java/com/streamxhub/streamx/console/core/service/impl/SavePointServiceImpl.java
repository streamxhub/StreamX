/*
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
package com.streamxhub.streamx.console.core.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.streamxhub.streamx.common.conf.ConfigConst;
import com.streamxhub.streamx.common.fs.FsOperator;
import com.streamxhub.streamx.console.base.domain.Constant;
import com.streamxhub.streamx.console.base.domain.RestRequest;
import com.streamxhub.streamx.console.base.exception.ServiceException;
import com.streamxhub.streamx.console.base.util.CommonUtils;
import com.streamxhub.streamx.console.base.util.SortUtils;
import com.streamxhub.streamx.console.core.dao.SavePointMapper;
import com.streamxhub.streamx.console.core.entity.SavePoint;
import com.streamxhub.streamx.console.core.enums.CheckPointType;
import com.streamxhub.streamx.console.core.service.SavePointService;
import com.streamxhub.streamx.console.core.service.SettingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author benjobs
 */
@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class SavePointServiceImpl extends ServiceImpl<SavePointMapper, SavePoint> implements SavePointService {

    @Autowired
    private SettingService settingService;

    @Autowired
    private FsOperator fsOperator;

    @Override
    public void obsolete(Long appId) {
        this.baseMapper.obsolete(appId);
    }


    @Override
    public boolean save(SavePoint entity) {
        this.expire(entity);
        this.obsolete(entity.getAppId());
        return super.save(entity);
    }

    private void expire(SavePoint entity) {
        int cpThreshold = Integer.parseInt(
            settingService
                .getFlinkDefaultConfig()
                .getOrDefault("state.checkpoints.num-retained", "1")
        );

        if (CheckPointType.CHECKPOINT.equals(CheckPointType.of(entity.getType()))) {
            cpThreshold = cpThreshold - 1;
        }

        if (cpThreshold == 0) {
            this.baseMapper.expireAll(entity.getAppId());
        } else {
            LambdaQueryWrapper<SavePoint> queryWrapper = new QueryWrapper<SavePoint>().lambda();
            queryWrapper.select(SavePoint::getTriggerTime)
                .eq(SavePoint::getAppId, entity.getAppId())
                .eq(SavePoint::getType, CheckPointType.CHECKPOINT.get())
                .orderByDesc(SavePoint::getTriggerTime)
                .last("limit 0," + cpThreshold + 1);

            List<SavePoint> savePointList = this.baseMapper.selectList(queryWrapper);
            if (!savePointList.isEmpty() && savePointList.size() > cpThreshold) {
                SavePoint savePoint = savePointList.get(cpThreshold - 1);
                this.baseMapper.expire(entity.getAppId(), savePoint.getTriggerTime());
            }
        }
    }

    @Override
    public SavePoint getLatest(Long id) {
        return this.baseMapper.getLatest(id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean delete(Long id) throws ServiceException {
        SavePoint savePoint = getById(id);
        try {
            if (CommonUtils.notEmpty(savePoint.getPath())) {
                fsOperator.delete(savePoint.getPath());
            }
            removeById(id);
            return true;
        } catch (Exception e) {
            throw new ServiceException(e.getMessage());
        }
    }

    @Override
    public IPage<SavePoint> page(SavePoint savePoint, RestRequest request) {
        Page<SavePoint> page = new Page<>();
        SortUtils.handlePageSort(request, page, "trigger_time", Constant.ORDER_DESC, false);
        return this.baseMapper.page(page, savePoint.getAppId());
    }

    @Override
    public void removeApp(Long appId) {
        baseMapper.removeApp(appId);
        fsOperator.delete(ConfigConst.APP_SAVEPOINTS().concat("/").concat(appId.toString()));
    }
}
