package justspark;

import demo.engine.SparkYarnJob;
import demo.engine.engine.type.EngineType;
import demo.engine.model.Args;
import prepare.Prepare;

import java.io.IOException;

/**
 * @Author: fansy
 * @Time: 2018/12/19 17:22
 * @Email: fansy1990@foxmail.com
 */
public class SparkNothingDemo {
    static String mainClass = "justspark.SparkNothing";
    public static void main(String[] args) throws IOException {
        Prepare.uploadJar();
        first();
        second();
    }

    public static void first() {
        String appName = "spark nothing test-true";
        String[] arguments = {"true", "default.spark_nothing", appName};
        Args innerArgs = Args.getArgs(appName, mainClass, arguments, EngineType.SPARK);
        SparkYarnJob.runAndMonitor(innerArgs);
    }
    public static void second() {
        String appName = "spark nothing test-false";
        String[] arguments = {"false", "default.spark_nothing", appName};
        Args innerArgs = Args.getArgs(appName, mainClass, arguments, EngineType.SPARK);
        SparkYarnJob.runAndMonitor(innerArgs);
    }

}
