package com.exampleredshift.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@Component
@PropertySource("classpath:application.properties")
public class AwsLambda implements RequestHandler<S3EventNotification, String> {

    @Value("${dbURL}")
    private String dbURL;
    @Value("${MasterUsername}")
    private String MasterUsername ;
    @Value("${MasterUserPassword}")
    private String MasterUserPassword;

    @Override
    public String handleRequest(S3EventNotification object, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Start!!! " + object.toJson());
        Connection conn = null;
        Statement statement = null;


        try {
            Class.forName("com.amazon.redshift.jdbc42.Driver");
            Properties props = new Properties();
            logger.log("dbURL : "+dbURL);
            logger.log("MasterUsername : " + MasterUsername);
            logger.log("MasterUserPassword : "+MasterUserPassword);
            props.setProperty("user", MasterUsername);
            props.setProperty("password", MasterUserPassword);

            logger.log("connecting to database...");


            conn = DriverManager.getConnection(dbURL, props);
            //conn.setAutoCommit(false);

            logger.log("Connection made!");

            statement = conn.createStatement();
            String schema = "public";
            String keyName = object.getRecords().get(0).getS3().getObject().getKey();
            String fileName = object.getRecords().get(0).getS3().getObject().getKey().split("/")[0];
            logger.log(fileName);

            String bucketName = object.getRecords().get(0).getS3().getBucket().getName();
            String selectQuery = "select * from dev.public.status where fileName like '" + fileName + "%'" + " and bucketName = '" + bucketName + "'";
            logger.log(selectQuery);
            ResultSet rs = statement.executeQuery(selectQuery);

            String inputCommand = null;
            String accessKey = null;
            String secretKey = null;
            while (rs.next()) {
                inputCommand = rs.getString("inputCommand");
                accessKey = rs.getString("accessKey");
                secretKey = rs.getString("accessSecret");
                logger.log("inputCommand " + inputCommand);

            }

            if (null != inputCommand) {
                String command = inputCommand + " 's3://" + bucketName + "/" + keyName + "' " +
                        "CREDENTIALS 'aws_access_key_id=" + accessKey + ";aws_secret_access_key=" + secretKey + "' " +
                        "CSV DELIMITER ',' ignoreheader 1";

                logger.log("Executing...");

                statement.executeUpdate(command);
                //conn.commit();
            }
            logger.log("That's all copy using simple JDBC.");
            statement.close();
            conn.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return "false";


    }
}
