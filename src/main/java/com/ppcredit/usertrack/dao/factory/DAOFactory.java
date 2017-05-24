package com.ppcredit.usertrack.dao.factory;

import com.ppcredit.usertrack.dao.ITaskDAO;
import com.ppcredit.usertrack.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {

        return new TaskDAOImpl();
    }

}
