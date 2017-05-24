package com.ppcredit.usertrack.test;

/**
 *
 */
public class ProcessTest {

    public static void main(String[] args){

        String scriptPath = "/home/beifeng/test.sh" ;

        // 创建Process实例
       // Process process = null ;

        // 执行命令
        //String execCmd = "/bin/chmod 777 " + scriptPath ;

        try{
            // 给脚本赋以执行权限
            //process = Runtime.getRuntime().exec(execCmd);

           // process.waitFor();

            String execCmd2 = "/bin/sh " + scriptPath  ; // 如果有参数，如下： + " " + pram

            Runtime.getRuntime().exec(execCmd2).waitFor();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
