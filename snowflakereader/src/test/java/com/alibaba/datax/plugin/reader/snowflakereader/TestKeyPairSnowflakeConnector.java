package com.alibaba.datax.plugin.reader.snowflakereader;

import java.util.Properties;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.util.Base64;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.KeyFactory;
import java.security.PrivateKey;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import static java.lang.Thread.sleep;

public class TestKeyPairSnowflakeConnector
{
    public static void main(String[] args)
            throws Exception
    {
        File f = new File("c:\\config\\rsa_key.p8");
        FileInputStream fis = new FileInputStream(f);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f.length()];
        dis.readFully(keyBytes);
        dis.close();

        String encrypted = new String(keyBytes);
        String passphrase = "*********";
        encrypted = encrypted.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "");
        encrypted = encrypted.replace("-----END ENCRYPTED PRIVATE KEY-----", "");
        System.out.println("encrypted:{}"+ encrypted);
        EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
        PBEKeySpec keySpec = new PBEKeySpec(passphrase.toCharArray());
        SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
        PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey encryptedPrivateKey = keyFactory.generatePrivate(encodedKeySpec);

        //String url = "jdbc:snowflake://vcg_dm.west-europe.azure.snowflakecomputing.com?warehouse=SBX_APD_ETL_WHS&JDBC_QUERY_RESULT_FORMAt=JSON";
        String url = "jdbc:snowflake://vcg_dm.west-europe.azure.snowflakecomputing.com/?JDBC_QUERY_RESULT_FORMAT=JSON&&tracing=ALL&warehouse=SBX_APD_ETL_WHS&db=SNOWFLAKE_SAMPLE_DATA&schema=TPCH_SF1&account=vcg_dm";
        Properties prop = new Properties();
        prop.put("user", "SBX_APD_RED_ETL_USR");
        //prop.put("account", "vcg_dm");
        prop.put("privateKey", encryptedPrivateKey);
        //prop.put("warehouse","SBX_APD_ETL_WHS");
        //prop.put("db","SNOWFLAKE_SAMPLE_DATA");
        //prop.put("schema","TPCH_SF1");

        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        Connection conn = DriverManager.getConnection(url, prop);


        conn = DriverManager.getConnection(url, prop);
        Statement stat = conn.createStatement();
        ResultSet res = stat.executeQuery("select O_ORDERKEY from ORDERS limit 10");
        res.next();
        System.out.println(res.getString(1));
        conn.close();
    }
}

