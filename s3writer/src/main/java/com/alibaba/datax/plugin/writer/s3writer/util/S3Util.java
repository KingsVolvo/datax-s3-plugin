package com.alibaba.datax.plugin.writer.s3writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.s3writer.Key;
import com.alibaba.datax.plugin.writer.s3writer.S3WriterErrorCode;
import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chochen on 2021/5/31.
 */
public class S3Util {

    private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

    public static AmazonS3 initS3Client(Configuration conf) {
        String regionStr = conf.getString(Key.REGION);
        Regions region = Regions.fromName(regionStr);
        String accessId = conf.getString(Key.ACCESSID);
        String accessKey = conf.getString(Key.ACCESSKEY);
        String accessToken = conf.getString(Key.ACCESSTOKEN);
        AmazonS3 client = null;
        try {
            if (Strings.isNullOrEmpty(accessToken)) {
                LOG.info("BasicAWSCredentials begins, accessToken is null.");
                AWSCredentials awsCreds = new BasicAWSCredentials(accessId, accessKey);
                client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(region).build();
            } else {
                LOG.debug("BasicSessionCredentials begins, accessToken is not null. accessId:{},accessKey:{},accessToken:{}", accessId, accessKey, accessToken);
                AWSCredentialsProvider awsCredsProvider = new AWSStaticCredentialsProvider(new BasicSessionCredentials(accessId, accessKey, accessToken));
                client = AmazonS3ClientBuilder.standard().withCredentials(awsCredsProvider).withRegion(region).build();
            }

        } catch (IllegalArgumentException e) {
            LOG.info("ERROR:{}" ,e);
            throw DataXException.asDataXException(
                    S3WriterErrorCode.ILLEGAL_VALUE, e.getMessage());
        }
        return client;
    }

    public static AmazonS3 initS3ClientByAssumeRole(Configuration conf) {
        LOG.info("initS3ClientByAssumeRole begin");
        //1 Get exist role
        InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(true);
        AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard().withCredentials(credentialsProvider).build();

        //2 Assume new role
        String roleArnRead = conf.getString(Key.ROLEARN);
        String roleSessionName = conf.getString(Key.ROLESESSIONNAME);
        int roleSessionDuration = conf.getInt(Key.SESSIONDURATIONSECONDS);
        STSAssumeRoleSessionCredentialsProvider stsAssumeRoleProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArnRead, roleSessionName).withRoleSessionDurationSeconds(roleSessionDuration).withStsClient(sts).build();
        AWSSessionCredentials newCredentials = stsAssumeRoleProvider.getCredentials();
        conf.set(Key.ACCESSID, newCredentials.getAWSAccessKeyId());
        conf.set(Key.ACCESSKEY, newCredentials.getAWSSecretKey());
        conf.set(Key.ACCESSTOKEN, newCredentials.getSessionToken());
        AmazonS3 client = initS3Client(conf);
        LOG.info("initS3ClientByAssumeRole ends.");
        return client;
    }
}
