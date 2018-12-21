package statics;

import demo.engine.SparkYarnJob;
import demo.engine.engine.type.EngineType;
import demo.engine.model.Args;

import java.io.IOException;

/**
 * @Author: fansy
 * @Time: 2018/12/19 17:22
 * @Email: fansy1990@foxmail.com
 */
public class DescribeStaticsDemo {
    static String mainClass = "statics.DescribeStatics";
    public static void main(String[] args) throws IOException {
//        Prepare.uploadJar();
//        first();
        second();
    }

    public static void first() {
        String appName = "demo_30m statics";
        String[] arguments = {"default.demo_30m", "default.demo_30m_statics", appName};
        Args innerArgs = Args.getArgs(appName, mainClass, arguments, EngineType.SPARK);
        SparkYarnJob.runAndMonitor(innerArgs);
    }

    /**
     * 1个 ApplicationMaster ； 2个子节点Executor； 耗时4.7 mins
     */
    public static void second(){
        String appName = "demo_600w statics";
        String[] arguments = {"default.demo_600w", "default.demo_600w_statics", appName};
        Args innerArgs = Args.getArgs(appName, mainClass, arguments, EngineType.SPARK);
        SparkYarnJob.runAndMonitor(innerArgs);
    }
}
