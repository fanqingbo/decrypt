package com.ppcredit.usertrack.dao;

import com.ppcredit.usertrack.domain.Task;

/**
 * 任务管理DAO接口
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    Task findByTaskId(long taskid);
}
