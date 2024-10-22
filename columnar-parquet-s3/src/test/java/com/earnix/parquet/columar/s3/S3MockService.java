package com.earnix.parquet.columar.s3;

import com.adobe.testing.s3mock.S3MockApplication;
import software.amazon.awssdk.services.s3.S3Client;

public class S3MockService implements AutoCloseable {
    private final S3MockApplication s3MockApplication = S3MockUtils.createAndStartNewS3Mock();
    private final S3Client s3Client = S3MockUtils.getS3Client(s3MockApplication);

    public S3MockService() {
        s3Client.createBucket(builder -> builder.bucket(testBucket()));
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public String testBucket() {
        return "testbucket";
    }

    @Override
    public void close() {
        try {
            s3Client.close();
        } finally {
            try {
                s3MockApplication.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
