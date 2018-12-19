package prepare;

import demo.engine.SparkYarnJob;
import demo.engine.engine.type.EngineType;
import demo.engine.model.Args;
import demo.engine.model.SubmitResult;
import demo.utils.SparkUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 准备工作：
 * 1. 上传算法包
 * 2. 上传数据
 * 3. 执行数据导入到Hive
 * @Author: fansy
 * @Time: 2018/12/19 11:49
 * @Email: fansy1990@foxmail.com
 */
public class Prepare {
    private static Logger log = LoggerFactory.getLogger(Prepare.class);
    public static void main(String[] args) throws IOException {
//        uploadJar();
//        uploadData();
        load2Hive();
    }

    /**
     * 上传工具类
     * @param srcFile
     * @param dstFile
     * @param override
     * @throws IOException
     */
    public static void upload(String srcFile, String dstFile,boolean override) throws IOException {

        Path src = new Path(srcFile);
        Path dst = new Path("/user/root");
        SparkUtils.getFs().copyFromLocalFile(false,true,src,dst);
        log.info("Copy {} done!", src);
    }

    public static void uploadJar() throws IOException {
        String resourcePath = Prepare.class.getClassLoader().getResource(".").getPath();
        log.debug("Resource Path : {}", resourcePath);
        upload(resourcePath+"../test_demo-1.0-SNAPSHOT.jar","/user/root",true);
    }

    public static void uploadData() throws IOException {
        String resourcePath = Prepare.class.getClassLoader().getResource(".").getPath();
        log.debug("Resource Path : {}", resourcePath);
        upload(resourcePath+"data.csv","/user/root",true);
    }

    public static void load2Hive(){
        String mainClass = "prepare.Load2Hive";
        String[] arguments = {"/user/root/data.csv","default.demo"};
        Args innerArgs = Args.getArgs("Load data to Hive",mainClass,arguments, EngineType.SPARK);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
    }

}
