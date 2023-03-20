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

public class TestKeyPairSnowflakeConnector
{
    public static void main(String[] args)
            throws Exception
    {
       
    }
}

