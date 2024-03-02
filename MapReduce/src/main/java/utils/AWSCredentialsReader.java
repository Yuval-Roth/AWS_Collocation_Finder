package utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.lambda.model.EC2AccessDeniedException;
import exceptions.AWSCredentialsReaderException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AWSCredentialsReader implements AWSCredentialsProvider {
    private final Map<String,String> credentialsMap;

    public AWSCredentialsReader(String pathToCredentials) throws AWSCredentialsReaderException {
        try {
            credentialsMap = new HashMap<>();
            try (BufferedReader varsFile = new BufferedReader(new FileReader(pathToCredentials))) {
                String line;
                while ((line = varsFile.readLine()) != null){
                    if(line.contains("=")){
                        //break the lines into key & value
                        String key = line.substring(0,line.indexOf("=")).strip();
                        String value = line.substring((line.indexOf("=")+1)).strip();
                        credentialsMap.put(key,value);
                    }
                }
            }

            validateCredentials();
        } catch (IOException e) {
            if(e instanceof FileNotFoundException){
                throw new AWSCredentialsReaderException("""
                        Credentials file not found.
                        Make sure the credentials file exists in the following path:
                        %s""");
            }
            throw new RuntimeException(e);
        }
    }

    private void validateCredentials() throws AWSCredentialsReaderException {

        // Check if the credentials file is in the correct format
        if(getAccessKeyId() == null || getSecretAccessKey() == null || getSessionToken() == null){
            throw new AWSCredentialsReaderException("""
                    Invalid credentials file.
                    Please make sure the credentials file is in the following format:
                    aws_access_key_id = <your access key>
                    aws_secret_access_key = <your secret key>
                    aws_session_token = <your session token>""");
        }

        // Test the credentials by making a call to AWS
        try {
            AmazonEC2 ec2Client = AmazonEC2Client.builder()
                    .withCredentials(this)
                    .withRegion("us-east-1")
                    .build();
            ec2Client.describeInstances();
        } catch (EC2AccessDeniedException e) {
            throw new AWSCredentialsReaderException("""
                    Credentials were rejected by AWS.
                    Please make sure the credentials are up to date.""");
        }
    }

    private String getAccessKeyId(){
        return credentialsMap.get("aws_access_key_id");
    }

    private String getSecretAccessKey(){
        return credentialsMap.get("aws_secret_access_key");
    }

    private String getSessionToken(){
        return credentialsMap.get("aws_session_token");
    }

    @Override
    public AWSCredentials getCredentials() {
        return new BasicSessionCredentials(getAccessKeyId(), getSecretAccessKey(), getSessionToken());
    }

    @Override
    public void refresh() {}
}

