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
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.streamxhub.streamx.common.conf.ConfigConst;
import com.streamxhub.streamx.common.fs.FsOperator;
import com.streamxhub.streamx.common.util.ThreadUtils;
import com.streamxhub.streamx.console.base.domain.Constant;
import com.streamxhub.streamx.console.base.domain.RestRequest;
import com.streamxhub.streamx.console.base.exception.ServiceException;
import com.streamxhub.streamx.console.base.util.SortUtils;
import com.streamxhub.streamx.console.core.dao.ApplicationBackUpMapper;
import com.streamxhub.streamx.console.core.entity.Application;
import com.streamxhub.streamx.console.core.entity.ApplicationBackUp;
import com.streamxhub.streamx.console.core.entity.ApplicationConfig;
import com.streamxhub.streamx.console.core.entity.FlinkSql;
import com.streamxhub.streamx.console.core.enums.DeployState;
import com.streamxhub.streamx.console.core.enums.EffectiveType;
import com.streamxhub.streamx.console.core.service.*;
import com.streamxhub.streamx.console.core.task.FlinkTrackingTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author benjobs
 */
@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationBackUpServiceImpl
        extends ServiceImpl<ApplicationBackUpMapper, ApplicationBackUp>
        implements ApplicationBackUpService {

    @Autowired
    private ApplicationService applicationService;

    @Autowired
    private ApplicationConfigService configService;

    @Autowired
    private EffectiveService effectiveService;

    @Autowired
    private FlinkSqlService flinkSqlService;

    @Autowired
    private FsOperator fsOperator;

    private ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2,
            200,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streamx-rollback-executor"),
            new ThreadPoolExecutor.AbortPolicy()
    );


    @Override
    public IPage<ApplicationBackUp> page(ApplicationBackUp backUp, RestRequest request) {
        Page<ApplicationBackUp> page = new Page<>();
        SortUtils.handlePageSort(request, page, "create_time", Constant.ORDER_DESC, false);
        return this.baseMapper.page(page, backUp.getAppId());
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public void rollback(ApplicationBackUp backParam) {
        /**
         * 备份文件不存在
         */
        if (!fsOperator.exists(backParam.getPath())) {
            return;
        }
        executorService.execute(() -> {
            try {
                FlinkTrackingTask.refreshTracking(backParam.getAppId(), () -> {
                    // 备份文件存在则执行回滚
                    // 1) 在回滚时判断当前生效的项目是否需要备份.如需要则先执行备份...
                    Application application = applicationService.getById(backParam.getAppId());
                    if (backParam.isBackup()) {
                        application.setBackUpDescription(backParam.getDescription());
                        backup(application);
                    }

                    //2) 恢复 配置和SQL

                    //如果正在运作,则设置Latest
                    if (application.isRunning()) {
                        //  回滚到备份的配置....
                        configService.setLatestOrEffective(true, backParam.getId(), backParam.getAppId());
                        // 如FlinkSQL任务则回滚sql语句和依赖
                        if (application.isFlinkSqlJob()) {
                            //TODO rollback
                            //flinkSqlService.setCandidateOrEffective(true,backParam.getAppId(), backParam.getSqlId());
                        }
                        //
                    } else {
                        // 回滚到备份的配置....
                        effectiveService.saveOrUpdate(backParam.getAppId(), EffectiveType.CONFIG, backParam.getId());
                        // 如FlinkSQL任务则回滚sql语句和依赖
                        if (application.isFlinkSqlJob()) {
                            effectiveService.saveOrUpdate(backParam.getAppId(), EffectiveType.FLINKSQL, backParam.getSqlId());
                        }
                    }

                    // 4) 删除当前的有效项目工程文件(注意:该操作如果整个回滚失败,则要恢复...)
                    fsOperator.delete(application.getAppHome().getAbsolutePath());

                    try {
                        // 5)将备份的文件copy到有效项目目录下.
                        fsOperator.copyDir(backParam.getPath(), application.getAppHome().getAbsolutePath());
                    } catch (Exception e) {
                        //1. TODO: 如果失败了,则要恢复第4部操作.

                        //2. throw e
                        throw e;
                    }

                    // 6) 更新重启状态
                    try {
                        applicationService.update(new UpdateWrapper<Application>()
                                .lambda()
                                .eq(Application::getId, application.getId())
                                .set(Application::getDeploy, DeployState.NEED_RESTART_AFTER_ROLLBACK.get())
                        );
                    } catch (Exception e) {
                        //1. TODO: 如果失败,则要恢复第4第5步操作.

                        //2. throw e
                        throw e;
                    }
                    return null;
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void revoke(Application application) {
        ApplicationBackUp backup = baseMapper.getLastBackup(application.getId());
        assert backup != null;
        String path = backup.getPath();
        fsOperator.move(path, ConfigConst.APP_WORKSPACE());
        removeById(backup.getId());
    }

    @Override
    public void removeApp(Long appId) {
        baseMapper.removeApp(appId);
        fsOperator.delete(
            ConfigConst.APP_BACKUPS().concat("/").concat(appId.toString())
        );
    }

    @Override
    public void rollbackFlinkSql(Application application, FlinkSql sql) {
        ApplicationBackUp backUp = getFlinkSqlBackup(application.getId(), sql.getId());
        assert backUp != null;
        if (!fsOperator.exists(backUp.getPath())) {
            return;
        }
        try {
            FlinkTrackingTask.refreshTracking(backUp.getAppId(), () -> {
                // 回滚 config 和 sql
                effectiveService.saveOrUpdate(backUp.getAppId(), EffectiveType.CONFIG, backUp.getId());
                effectiveService.saveOrUpdate(backUp.getAppId(), EffectiveType.FLINKSQL, backUp.getSqlId());

                // 2) 删除当前项目
                fsOperator.delete(application.getAppHome().getAbsolutePath());
                try {
                    // 5)将备份的文件copy到有效项目目录下.
                    fsOperator.copyDir(backUp.getPath(), application.getAppHome().getAbsolutePath());
                } catch (Exception e) {
                    throw e;
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isFlinkSqlBacked(Long appId, Long sqlId) {
        LambdaQueryWrapper<ApplicationBackUp> queryWrapper = new QueryWrapper<ApplicationBackUp>().lambda();
        queryWrapper.eq(ApplicationBackUp::getAppId, appId)
                .eq(ApplicationBackUp::getSqlId,sqlId);
        return baseMapper.selectCount(queryWrapper) > 0;
    }

    private ApplicationBackUp getFlinkSqlBackup(Long appId, Long sqlId) {
        return baseMapper.getFlinkSqlBackup(appId, sqlId);
    }

    @Override
    public Boolean delete(Long id) throws ServiceException {
        ApplicationBackUp backUp = getById(id);
        try {
            fsOperator.delete(backUp.getPath());
            removeById(id);
            return true;
        } catch (Exception e) {
            throw new ServiceException(e.getMessage());
        }
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public void backup(Application application) {
        //1) 基础的配置文件备份
        File appHome = application.getAppHome();
        if (fsOperator.exists(appHome.getPath())) {
            // 3) 需要备份的做备份,移动文件到备份目录...
            ApplicationConfig config = configService.getEffective(application.getId());
            if (config != null) {
                application.setConfigId(config.getId());
            }

            //2) FlinkSQL任务需要备份sql和依赖.
            int version = 1;
            if (application.isFlinkSqlJob()) {
                FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(),false);
                assert flinkSql != null;
                application.setSqlId(flinkSql.getId());
                version = flinkSql.getVersion();
            } else if(config != null){
                version = config.getVersion();
            }

            ApplicationBackUp applicationBackUp = new ApplicationBackUp(application);
            applicationBackUp.setVersion(version);

            this.save(applicationBackUp);
            fsOperator.mkdirs(applicationBackUp.getPath());
            fsOperator.move(appHome.getPath(), applicationBackUp.getPath());
        }
    }

}
