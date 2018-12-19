package demo.utils;

import demo.engine.engine.type.EngineType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.apache.spark.deploy.yarn.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static demo.utils.CommonConstants.*;


/**
 * Spark / Hadoop 工具类
 * <p>
 * Created by fansy on 2017/2/4.
 */
public class SparkUtils {
    private final static Logger logger = LoggerFactory.getLogger(SparkUtils.class);
    private static Configuration conf = null;
    private static FileSystem fs = null;
    private static YarnClient client = null;
    private static Map<String,String > configurationProperties = new HashMap<>();
    private static RestSubmissionClient restSubmissionClient;
    /**
     * static 定义在 后面
     */
    static{
        updateProperties();

    }

    /**
     * 初始化配置文件
     */
    private static void updateProperties(){
        // 1. 获取hadoop.properties 配置
        configurationProperties.putAll(PropertiesUtil.getProperties(HADOOP_PROPERTIES));

        // 2. 获取 spark.properties
        configurationProperties.putAll(PropertiesUtil.getProperties(PLATFORM_PREFIX+
                getValue("platform") +"/" + SPARK_PROPERTIES));
        // 3. 获取 yarn.properties
        configurationProperties.putAll(PropertiesUtil.getProperties(PLATFORM_PREFIX+
                getValue("platform") +"/" + YARN_PROPERTIES));
        // set the submit user to the configuration file assigned
        System.setProperty("HADOOP_USER_NAME", getValue("yarn.submit.user"));
    }

    /**
     * 获取配置文件参数值
     * @param key
     * @return
     */
    public static String getValue(String key){
        if(configurationProperties.containsKey(key)){
            return configurationProperties.get(key);
        }
        return null ;
    }

    /**
     * 获取SparkConf
     * @param engineType
     * @return
     */
    public static SparkConf getSparkConf(EngineType engineType){
        SparkConf sparkConf = new SparkConf();
        switch (engineType){
            case YARN:
                sparkConf.set("spark.yarn.jar", getValue("yarn.spark.assemble.jar"));
                sparkConf.set("spark.yarn.scheduler.heartbeat.interval-ms",
                        getValue("yarn.spark.yarn.scheduler.heartbeat.interval-ms"));
                sparkConf.set("spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH",
                        getValue("yarn.spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH"));
                sparkConf.set("spark.driver.extraJavaOptions", getValue("yarn.spark.driver.extraJavaOptions"));
                break;
            case SPARK:
                sparkConf.set("spark.master", getValue("spark.master"));
                sparkConf.set("spark.driver.memory", getValue("spark.driver.memory"));
                sparkConf.set("spark.executor.memory", getValue("spark.executor.memory"));
                sparkConf.set("spark.cores.max", getValue("spark.cores.max"));
                sparkConf.set("spark.executor.cores", getValue("spark.executor.cores"));
                sparkConf.set("spark.executor.extraClassPath", getValue("spark.executor.extraClassPath"));
                sparkConf.set("spark.driver.extraClassPath",getValue("spark.executor.extraClassPath"));
                // the code below should not be change
                sparkConf.set("spark.submit.deployMode","cluster");
                sparkConf.set("spark.driver.supervise","false");
                sparkConf.set("spark.files", getValue("spark.files"));
                break;
            default:
                logger.warn("Not support type:{}",engineType.name());
        }
        return sparkConf;
    }


    /**
     * 获取配置文件所在local路径
     * @return
     */
    private static File getConfigurationFileParent(){
        URL url = SparkUtils.class.getClassLoader().getResource(PLATFORM_PREFIX + getValue("platform") );
        return new File(url.getFile());
    }
    /**
     * 获取Configuration
     *
     * @return
     */
    public static Configuration getConf() {
        try {
            if (conf == null) {
                conf = new Configuration();
                conf.set("mapreduce.app-submission.cross-platform",
                        getValue("mapreduce.app-submission.cross-platform"));// 配置使用跨平台提交任务
                File file = getConfigurationFileParent();
                if (!file.exists() || !file.isDirectory()) {
                    logger.error("路径{}不是目录或不存在！", file.getAbsolutePath());
                    return conf;
                }
                for (File f : file.listFiles()) {
                    if (f.getAbsolutePath().lastIndexOf("xml") != -1) {
                        logger.info("添加{}资源！", f.getName());

                        conf.addResource(f.toURI().toURL());
                    } else {
                        logger.debug("资源{}不是以xml结尾！", f.getAbsolutePath());
                    }
                }
                /**
                 * CDH 集群远程提交Spark任务到YARN集群，出现
                 * java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
                 * 异常，需要设置mapreduce.application.classpath 参数 或
                 * yarn.application.classpath 参数
                 */
                conf.set("yarn.application.classpath", getValue("yarn.spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conf;
    }



    /**
     * 获取YARN引擎客户端
     *
     * @return
     */
    public static YarnClient getClient() {
        if (client == null) {
            client = YarnClient.createYarnClient();
            client.init(getConf());
            client.start();
        }
        return client;
    }

    public static FileSystem getFs()  {
        if(fs == null){
            try {
                fs = FileSystem.get(getConf());
            }catch (IOException e){
                if(e.getMessage().contains("winutils.exe")){
                    logger.info("Winutils error.");
                }
            }
        }
        return fs;
    }

    /**
     * 获取Spark引擎客户端
     * @return
     */
    public static RestSubmissionClient getRestSubmissionClient(){
        if(restSubmissionClient == null){
            restSubmissionClient =  new RestSubmissionClient(getValue("spark.master"));
        }
        return restSubmissionClient;
    }


    /**
     * 当任务运行成功或失败或被杀死，则返回true（不需要再次检查）
     * 如果是Running状态，则还需要再次检查任务状态
     *
     * @param jobIdStr
     * @return
     */
    public static FinalApplicationStatus getFinalStatus(String jobIdStr) throws IOException, YarnException {
        ApplicationId jobId = ConverterUtils.toApplicationId(jobIdStr);
        ApplicationReport appReport = null;
        try {
            appReport = getClient().getApplicationReport(jobId);
            return appReport.getFinalApplicationStatus();
        } catch (YarnException | IOException e) {
            e.printStackTrace();
            throw e;
        }
    }


    private static void cleanupStagingDir(ApplicationId applicationId) throws IOException {
        String appStagingDir = Client.SPARK_STAGING() + Path.SEPARATOR + applicationId.toString();
        Path stagingDirPath = new Path(appStagingDir);
        FileSystem fs = SparkUtils.getFs();
        if (fs.exists(stagingDirPath)) {
            logger.info("Deleting staging directory " + stagingDirPath);
                fs.delete(stagingDirPath, true);
        }
    }
    public static void cleanupStagingDir(String appId) {
        final long waitForMinutes = Long.parseLong(getValue("yarn.job.clean.interval"));
        Runnable runnable = () -> {
            try {
                logger.info("准备清空Spark任务：{} 中间日志，{}分钟后开始...", appId,waitForMinutes);
                Thread.sleep(waitForMinutes * 60 * 1000);// wait for 3 minutes
                cleanupStagingDir(ConverterUtils
                        .toApplicationId(appId));
            } catch (Exception e) {
                logger.error("运行删除spark中间日志任务失败！");
            }
            logger.info("清空Spark任务：{} 中间日志完成", appId);
        };
        new Thread(runnable).start();
    }


}
