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
package com.streamxhub.console.core.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.streamxhub.common.conf.ConfigConst;
import com.streamxhub.common.conf.ParameterCli;
import com.streamxhub.common.util.DeflaterUtils;
import com.streamxhub.common.util.HdfsUtils;
import com.streamxhub.common.util.Utils;
import com.streamxhub.common.util.YarnUtils;
import com.streamxhub.console.base.domain.Constant;
import com.streamxhub.console.base.domain.RestRequest;
import com.streamxhub.console.base.properties.StreamXProperties;
import com.streamxhub.console.base.utils.CommonUtil;
import com.streamxhub.console.base.utils.SortUtil;
import com.streamxhub.console.core.dao.ApplicationMapper;
import com.streamxhub.console.core.entity.*;
import com.streamxhub.console.core.enums.AppExistsState;
import com.streamxhub.console.core.enums.ApplicationType;
import com.streamxhub.console.core.enums.DeployState;
import com.streamxhub.console.core.service.*;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.streamxhub.console.core.enums.FlinkAppState;
import com.streamxhub.console.system.authentication.ServerUtil;
import com.streamxhub.flink.submit.FlinkSubmit;
import com.streamxhub.flink.submit.SubmitInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.jar.Manifest;

/**
 * @author benjobs
 */
@Slf4j
@Service("applicationService")
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationServiceImpl extends ServiceImpl<ApplicationMapper, Application> implements ApplicationService {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ApplicationBackUpService backUpService;

    @Autowired
    private ApplicationConfigService configService;

    @Autowired
    private SavePointService savePointService;

    @Autowired
    private ServerUtil serverUtil;

    @Autowired
    private StreamXProperties properties;

    @Override
    public IPage<Application> list(Application paramOfApp, RestRequest request) {
        Page<Application> page = new Page<>();
        SortUtil.handlePageSort(request, page, "create_time", Constant.ORDER_DESC, false);
        return this.baseMapper.findApplication(page, paramOfApp);
    }

    @Override
    public String getYarnName(Application paramOfApp) {
        String[] args = new String[2];
        args[0] = "--name";
        args[1] = paramOfApp.getConfig();
        return ParameterCli.read(args);
    }

    /**
     * 检查当前的jobName在表和yarn中是否已经存在
     *
     * @param paramOfApp
     * @return
     */
    @Override
    public AppExistsState checkExists(Application paramOfApp) {
        QueryWrapper<Application> queryWrapper = new QueryWrapper();
        queryWrapper.eq("job_name", paramOfApp.getJobName());
        int count = this.baseMapper.selectCount(queryWrapper);
        boolean exists = YarnUtils.isContains(paramOfApp.getJobName());
        if (count == 0 && !exists) {
            return AppExistsState.NO;
        }
        return exists ? AppExistsState.IN_YARN : AppExistsState.IN_DB;
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public boolean create(Application paramOfApp) {
        //配置文件中配置的yarnName..
        paramOfApp.setUserId(serverUtil.getUser().getUserId());
        paramOfApp.setState(FlinkAppState.CREATED.getValue());
        paramOfApp.setCreateTime(new Date());
        if (paramOfApp.getAppType() == ApplicationType.STREAMX_FLINK.getType()) {
            configService.create(paramOfApp);
        }

        boolean saved = save(paramOfApp);
        if (saved) {
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    paramOfApp.setBackUp(false);
                    paramOfApp.setRestart(false);
                    deploy(paramOfApp);
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return saved;
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public boolean update(Application paramOfApp) {
        //update other...
        Application application = getById(paramOfApp.getId());
        application.setJobName(paramOfApp.getJobName());
        application.setArgs(paramOfApp.getArgs());
        application.setOptions(paramOfApp.getOptions());
        application.setDynamicOptions(paramOfApp.getDynamicOptions());
        application.setDescription(paramOfApp.getDescription());
        //update config...
        if (application.getAppType() == ApplicationType.STREAMX_FLINK.getType()) {
            configService.update(paramOfApp);
        } else {
            application.setJar(paramOfApp.getJar());
            application.setMainClass(paramOfApp.getMainClass());
        }
        /**
         * 配置文件已更新
         */
        application.setDeploy(DeployState.CONF_UPDATED.get());
        this.baseMapper.updateById(application);
        return true;
    }

    @Override
    public void deploy(Application paramOfApp) throws Exception {
        Application application = getById(paramOfApp.getId());
        Boolean isRunning = application.getState() == FlinkAppState.RUNNING.getValue();

        //1) 需要重启的先停止服务
        if (paramOfApp.getRestart()) {
            stop(paramOfApp);
        } else if (!isRunning) {
            //不需要重启的并且未正在运行的,则更改状态为发布中....
            application.setState(FlinkAppState.DEPLOYING.getValue());
            updateState(application);
        }

        //2) deploying...
        application.setBackUpDescription(paramOfApp.getBackUpDescription());
        String workspaceWithModule = application.getWorkspace(true);
        if (HdfsUtils.exists(workspaceWithModule)) {
            ApplicationBackUp applicationBackUp = new ApplicationBackUp(application);
            //3) 需要备份的做备份...
            if (paramOfApp.getBackUp()) {
                backUpService.save(applicationBackUp);
                HdfsUtils.mkdirs(applicationBackUp.getPath());
                HdfsUtils.movie(workspaceWithModule, applicationBackUp.getPath());
            }
        }
        String workspace = application.getWorkspace(false);
        if (!HdfsUtils.exists(workspace)) {
            HdfsUtils.mkdirs(workspace);
        }
        File needUpFile = new File(application.getAppBase(), application.getModule());
        HdfsUtils.upload(needUpFile.getAbsolutePath(), workspace);

        //4) 更新发布状态,需要重启的应用则重新启动...
        if (paramOfApp.getRestart()) {
            //重新启动.
            start(paramOfApp);
            //将"需要重新发布"状态清空...
            application.setDeploy(DeployState.NONE.get());
            updateDeploy(application);
        } else {
            application.setDeploy(DeployState.NEED_START.get());
            updateDeploy(application);
            if (!isRunning) {
                application.setState(FlinkAppState.DEPLOYED.getValue());
                updateState(application);
            }
        }
    }

    @Override
    public void updateDeploy(Application application) {
        this.baseMapper.updateDeploy(application);
    }

    @Override
    public void closeDeploy(Application paramOfApp) {
        paramOfApp.setDeploy(DeployState.NONE.get());
        this.baseMapper.updateDeploy(paramOfApp);
    }

    @Override
    public String readConf(Application paramOfApp) throws IOException {
        File file = new File(paramOfApp.getConfig());
        String conf = FileUtils.readFileToString(file, "utf-8");
        return Base64.getEncoder().encodeToString(conf.getBytes());
    }

    @Override
    public Application getApp(Application paramOfApp) {
        Application application = this.baseMapper.getApp(paramOfApp);
        if (application.getConfig() != null) {
            String unzipString = DeflaterUtils.unzipString(application.getConfig());
            String encode = Base64.getEncoder().encodeToString(unzipString.getBytes());
            application.setConfig(encode);
        }
        String path = this.projectService.getAppConfPath(application.getProjectId(), application.getModule());
        application.setConfPath(path);
        return application;
    }

    @Override
    public String getMain(Application application) {
        Project project = new Project();
        project.setId(application.getProjectId());
        String modulePath = project.getAppBase().getAbsolutePath().concat("/").concat(application.getModule());
        File jarFile = new File(modulePath, application.getJar());
        Manifest manifest = Utils.getJarManifest(jarFile);
        String mainClass = manifest.getMainAttributes().getValue("Main-Class");
        return mainClass;
    }

    @Override
    public void updateState(Application application) {
        this.baseMapper.updateState(application);
    }

    @Override
    @Transactional(rollbackFor = {Exception.class})
    public void stop(Application paramOfApp) {
        Application application = getById(paramOfApp.getId());
        application.setState(FlinkAppState.CANCELLING.getValue());
        this.baseMapper.updateById(application);
        CommonUtil.localCache.put(paramOfApp.getId(), Long.valueOf(System.currentTimeMillis()));
        String savePointDir = FlinkSubmit.stop(properties.getNameService(), application.getAppId(), application.getJobId(), paramOfApp.getSavePointed(), paramOfApp.getDrain());
        if (paramOfApp.getSavePointed()) {
            SavePoint savePoint = new SavePoint();
            savePoint.setAppId(application.getId());
            savePoint.setLastest(true);
            savePoint.setSavePoint(savePointDir);
            savePoint.setCreateTime(new Date());
            //之前的配置设置为已过期
            this.savePointService.obsolete(application.getId());
            this.savePointService.save(savePoint);
        }
    }

    @Override
    public void updateMonitor(Application paramOfApp) {
        this.baseMapper.updateMonitor(paramOfApp);
    }

    @Override
    public boolean start(Application paramOfApp) throws Exception {
        final Application application = getById(paramOfApp.getId());
        assert application != null;
        Project project = projectService.getById(application.getProjectId());
        assert project != null;
        String workspaceWithSchemaAndNameService = "hdfs://".concat(properties.getNameService()).concat(ConfigConst.APP_WORKSPACE());

        String appConf, flinkUserJar;
        switch (application.getApplicationType()) {
            case STREAMX_FLINK:
                ApplicationConfig applicationConfig = configService.getActived(application.getId());
                String confContent = applicationConfig.getContent();
                String format = applicationConfig.getFormat() == 1 ? "yaml" : "prop";
                appConf = String.format("%s://%s", format, confContent);
                String classPath = String.format("%s/%s/%s/lib", workspaceWithSchemaAndNameService, application.getId(), application.getModule());
                flinkUserJar = String.format("%s/%s.jar", classPath, application.getModule());
                break;
            case APACHE_FLINK:
                appConf = String.format(
                        "json://{\"%s\":\"%s\"}",
                        ConfigConst.KEY_FLINK_APP_MAIN(),
                        application.getMainClass()
                );
                classPath = String.format("%s/%s/%s", workspaceWithSchemaAndNameService, application.getId(), application.getModule());
                flinkUserJar = String.format("%s/%s", classPath, application.getJar());
                break;
            default:
                throw new IllegalArgumentException("[StreamX] ApplicationType must be (STREAMX_FLINK|APACHE_FLINK)... ");
        }

        String savePointDir = null;
        if (paramOfApp.getSavePointed()) {
            if (paramOfApp.getSavePoint() == null) {
                SavePoint savePoint = savePointService.getLastest(paramOfApp.getId());
                if (savePoint != null) {
                    savePointDir = savePoint.getSavePoint();
                }
            } else {
                savePointDir = paramOfApp.getSavePoint();
            }
        }

        Map<String,Object> overrideOption = application.getOptionMap();

        if (CommonUtil.notEmpty(overrideOption)) {
            if (paramOfApp.getAllowNonRestored()) {
                overrideOption.put("allowNonRestoredState",true);
            }
        } else {
            if (paramOfApp.getAllowNonRestored()) {
                overrideOption = new HashMap<>(1);
                overrideOption.put("allowNonRestoredState",true);
            }
        }

        String[] dynamicOption = CommonUtil.notEmpty(application.getDynamicOptions())
                ? application.getDynamicOptions().split("\\s+")
                : new String[0];

        SubmitInfo submitInfo = new SubmitInfo(
                properties.getNameService(),
                flinkUserJar,
                application.getJobName(),
                appConf,
                application.getApplicationType().getName(),
                savePointDir,
                overrideOption,
                dynamicOption,
                application.getArgs()
        );

        ApplicationId appId = FlinkSubmit.submit(submitInfo);
        application.setAppId(appId.toString());
        /**
         * 一定要在flink job提交完毕才置状态...
         */
        application.setState(FlinkAppState.STARTING.getValue());
        application.setEndTime(null);
        this.baseMapper.updateById(application);
        return true;
    }

}