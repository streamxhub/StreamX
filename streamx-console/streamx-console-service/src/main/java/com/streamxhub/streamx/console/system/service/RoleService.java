package com.streamxhub.streamx.console.system.service;

import java.util.List;
import java.util.Set;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;
import com.streamxhub.streamx.console.base.domain.RestRequest;
import com.streamxhub.streamx.console.system.entity.Role;

public interface RoleService extends IService<Role> {

    Set<String> getUserRoleName(String username);

    IPage<Role> findRoles(Role role, RestRequest request);

    List<Role> findUserRole(String userName);

    Role findByName(String roleName);

    void createRole(Role role);

    void deleteRoles(String[] roleIds) throws Exception;

    void updateRole(Role role) throws Exception;
}