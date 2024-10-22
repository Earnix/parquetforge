package com.earnix.parquet.columar.s3;

import com.adobe.testing.s3mock.S3MockApplication;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class S3MockUtils {
    static S3MockApplication createAndStartNewS3Mock() {
        Map<String, Object> s3AppProps = new HashMap<>();

        // avoid port conflicts
        s3AppProps.put("server.port", 0);
        s3AppProps.put("http.port", 0);

        S3MockApplication s3MockApplication = S3MockApplication.start(s3AppProps);

        return s3MockApplication;
    }

    static String getS3Endpoint(S3MockApplication s3MockApplication) {
        return "http://127.0.0.1:" + s3MockApplication.getHttpPort();
    }

    static S3Client getS3Client(S3MockApplication s3MockApplication) {

        S3ClientBuilder builder = S3Client.builder()//
                .httpClientBuilder(software.amazon.awssdk.http.apache.ApacheHttpClient.builder()
                        .maxConnections(100));
        URI endpointURI;
        try {
            endpointURI = new URI(getS3Endpoint(s3MockApplication));
        } catch (URISyntaxException ex) {
            // shouldn't occur.
            throw new RuntimeException(ex);
        }
        return builder
                .endpointOverride(endpointURI) //
                .credentialsProvider(AnonymousCredentialsProvider.create()) //
                .forcePathStyle(true) //
                .region(Region.US_EAST_1) // who cares, as we're using an explicit endpoint
                .build();
    }
}
