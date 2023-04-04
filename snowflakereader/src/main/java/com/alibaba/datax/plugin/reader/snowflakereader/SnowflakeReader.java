package com.alibaba.datax.plugin.reader.snowflakereader;


import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.*;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;


public class SnowflakeReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.SnowFlake;
    private static final String KEY_PAIR = "keypair";

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE,
                    Integer.MIN_VALUE);
            this.originalConfig.set(Constant.FETCH_SIZE, fetchSize);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
             this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);
            String authType = this.originalConfig.getString(com.alibaba.datax.plugin.reader.snowflakereader.Key.AUTH_TYPE,StringUtils.EMPTY);
            if (authType.equalsIgnoreCase(KEY_PAIR)){
                String keyfilepath =  this.originalConfig.getString(com.alibaba.datax.plugin.reader.snowflakereader.Key.KEY_FILE_PATH,StringUtils.EMPTY);
                if (keyfilepath.equals(StringUtils.EMPTY)){
                    throw new IllegalArgumentException("Invalid key file path when auth type is keypair");
                }
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin...");
            List<Configuration> splitResult = this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
            /**
             * 在日志中告知用户,为什么实际datax切分跑的channel数会小于用户配置的channel数
             */
            if(splitResult.size() < adviceNumber){
                // 如果用户没有配置切分主键splitPk
                if(StringUtils.isBlank(this.originalConfig.getString(Key.SPLIT_PK, null))){
                    LOG.info("User has not configured splitPk.");
                }else{
                    // 用户配置了切分主键,但是切分主键可能重复太多,或者要同步的表的记录太少,无法切分成adviceNumber个task
                    LOG.info("User has configured splitPk. But the number of task finally split is smaller than that user has configured. " +
                            "The possible reasons are: 1) too many repeated splitPk values, 2) too few records.");
                }
            }
            LOG.info("split() ok and end...");
            return splitResult;
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }


    public static class Task extends Reader.Task{
        private static final Logger LOG = LoggerFactory
                .getLogger(SnowflakeReader.Task.class);
        private Configuration readerSliceConfig;
        private SnowflakeReaderTask snowflakeReaderTask;


        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig
                    .getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);
            String authType = this.readerSliceConfig.getString(Key.AUTH_TYPE);
            this.snowflakeReaderTask.startRead(this.readerSliceConfig,
                    recordSender, super.getTaskPluginCollector(), fetchSize,authType);
       }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.snowflakeReaderTask = new SnowflakeReaderTask(
                    DATABASE_TYPE,super.getTaskGroupId(),super.getTaskId());
            this.snowflakeReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void post() {
            this.snowflakeReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.snowflakeReaderTask.destroy(this.readerSliceConfig);
        }
    }

    public static class SnowflakeReaderTask extends CommonRdbmsReader.Task{

        private static final Logger LOG = LoggerFactory
                .getLogger(SnowflakeReader.SnowflakeReaderTask.class);

        /*Always the same as parent taskGroupId*/
        /**
        * Why do this, because I want rewrite startRead method, and taskGroupId,taskId can not access by subclass.
        **/
        private int taskGroupId;
        /*Always the same as parent taskGroupId*/
        private int taskId;

        private DataBaseType dataBaseType;


        public SnowflakeReaderTask(DataBaseType dataBaseType) {
            super(dataBaseType);
            this.dataBaseType = dataBaseType;
        }

        public SnowflakeReaderTask(DataBaseType dataBaseType, int taskGroupId, int taskId) {
            super(dataBaseType, taskGroupId, taskId);
            this.taskGroupId = taskGroupId;
            this.taskId = taskId;
            this.dataBaseType = DataBaseType.SnowFlake;
        }

        public void startRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize, String authType) {
            /*Same as CommonRdbmsReader.Task starRead value.*/
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
            String table = readerSliceConfig.getString(Key.TABLE);
            String jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
            String username = readerSliceConfig.getString(Key.USERNAME);
            String password = readerSliceConfig.getString(Key.PASSWORD);
            String mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");
            String basicMsg = String.format("jdbcUrl:[%s]", jdbcUrl);



            PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

            LOG.info("Begin to read record by Sql: [{}\n] {}.",
                    querySql, basicMsg);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();
            Connection conn = null;
            if(authType.equalsIgnoreCase(KEY_PAIR)){
                LOG.info("begin to use keypair authentication.");
                String keyFile = readerSliceConfig.getString(com.alibaba.datax.plugin.reader.snowflakereader.Key.KEY_FILE_PATH, "");
                try {
                    PrivateKey privateKey = generatePrivateKey(keyFile,password);
                    //conn = getKeyPairConnection(this.dataBaseType,jdbcUrl,username,privateKey,String.valueOf(172800000));
                    conn = DBUtil.getConnection(this.dataBaseType,jdbcUrl,username,privateKey);
                } catch (IOException e) {
                   throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,"IOException",e);
                } catch (NoSuchAlgorithmException e) {
                   throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,"NoSuchAlgorithmException",e);
                } catch (InvalidKeyException e) {
                   throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,"InvalidKeyException",e);
                } catch (InvalidKeySpecException e) {
                   throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,"InvalidKeySpecException",e);
                }

            }
            else {
                conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
                        username, password);
            }


            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
                    this.dataBaseType, basicMsg);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                //这个统计干净的result_Next时间
                PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n] {}.",
                        querySql, basicMsg);

            }catch (Exception e) {
                throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public static PrivateKey generatePrivateKey(String keyFile, String passphrase) throws IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidKeySpecException {
            File f = new File(keyFile);
            FileInputStream fis = new FileInputStream(f);
            DataInputStream dis = new DataInputStream(fis);
            byte[] keyBytes = new byte[(int) f.length()];
            dis.readFully(keyBytes);
            dis.close();

            String encrypted = new String(keyBytes);
            LOG.info("encrypted before replace, {}",encrypted);
            encrypted = encrypted.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "")
                    .replace("-----END ENCRYPTED PRIVATE KEY-----", "");
            LOG.info("encrypted after replace, {}",encrypted);
            EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
            PBEKeySpec keySpec = new PBEKeySpec(passphrase.toCharArray());
            SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
            PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey encryptedPrivateKey = keyFactory.generatePrivate(encodedKeySpec);
            return encryptedPrivateKey;
        }

        /**
         *
         * @param dataBaseType
         * @param jdbcUrl
         * @param username
         * @param password
         * @param socketTimeout 设置socketTimeout，单位ms，String类型
         * @return
         */
        public static Connection getKeyPairConnection(final DataBaseType dataBaseType,
                                               final String jdbcUrl, final String username, final PrivateKey password, final String socketTimeout) {

            try {
                return RetryUtil.executeWithRetry(new Callable<Connection>() {
                    @Override
                    public Connection call() throws Exception {
                        return connectWithKeyPairAuth(dataBaseType, jdbcUrl, username,
                                password, socketTimeout);
                    }
                }, 9, 1000L, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.CONN_DB_ERROR,
                        String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
            }
        }

        private static synchronized Connection connectWithKeyPairAuth(DataBaseType dataBaseType,
                                                       String url, String user, PrivateKey pass, String socketTimeout) {

            //ob10的处理
            if (url.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
                String[] ss = url.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
                }
                LOG.info("this is ob1_0 jdbc url.");
                user = ss[1].trim() +":"+user;
                url = ss[2].replace("jdbc:mysql:", "jdbc:oceanbase:");
                LOG.info("this is ob1_0 jdbc url. user="+user+" :url="+url);
            }

            Properties prop = new Properties();
            prop.put("user", user);
            prop.put("privateKey", pass);

            if (dataBaseType == DataBaseType.Oracle) {
                //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
                // unit ms
                prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
            }
            if (dataBaseType == DataBaseType.OceanBase) {
                url = url.replace("jdbc:mysql:", "jdbc:oceanbase:");
            }

            return connect(dataBaseType, url, prop);
        }

        private static synchronized Connection connect(DataBaseType dataBaseType,
                                                       String url, Properties prop) {
            try {
                LOG.info("For Class Name:{}, url:{}", dataBaseType.getDriverClassName(), url);
                Class.forName(dataBaseType.getDriverClassName());
                int TIMEOUT_SECONDS = 172800;
                DriverManager.setLoginTimeout(TIMEOUT_SECONDS);
                return DriverManager.getConnection(url, prop);
            } catch (Exception e) {
                throw RdbmsException.asConnException(dataBaseType, e, prop.getProperty("user"), null);
            }
        }
    }
}
