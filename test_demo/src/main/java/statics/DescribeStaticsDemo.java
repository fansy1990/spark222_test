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
    public static void main(String[] args) throws IOException {
//        Prepare.uploadJar();
        first();
    }

    public static void first() {
        String mainClass = "statics.DescribeStatics";
        String appName = "demo_30m statics";
        String[] arguments = {"default.demo_30m", "default.demo_30m_statics", appName};
        Args innerArgs = Args.getArgs(appName, mainClass, arguments, EngineType.SPARK);
        SparkYarnJob.runAndMonitor(innerArgs);
    }
}
