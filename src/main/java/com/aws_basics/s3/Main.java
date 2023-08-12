package com.aws_basics.s3;

import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;


public class Main {
    public static void main(String[] args) {

        S3Client s3Client = getS3Client(Constants.awsRegion);

        boolean bucketCreationStatus = createBucket(s3Client, createBucketName());

        if(bucketCreationStatus) {
            listBuckets(s3Client);
        }
    }

    //Create Unique Bucket name
    private static String createBucketName(){
        DateFormat dateObjectFormat = new SimpleDateFormat("ddMMyyyy-HHmmss");
        return Constants.buketNamePrefix + dateObjectFormat.format(Calendar.getInstance().getTime());
    }

    //Create S3Client with required permission
    private static S3Client getS3Client(Region region){
        return S3Client.builder().region(region).build();
    }


    //Method to create bucket
    public static boolean createBucket(S3Client s3Client, String bucketName) {

        try {
            System.out.println("Creating S3 bucket....");
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.createBucket(bucketRequest);

            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response.
            S3Waiter s3Waiter = s3Client.waiter();
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName + " is ready to use.");

            return true;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
        }
    }

    //Method to list of buckets in S3
    private static void listBuckets(S3Client s3Client){
        List<Bucket> bucketLists = s3Client.listBuckets().buckets();
        System.out.println("Listing all S3 buckets.....");
        for (Bucket bucket : bucketLists){
            System.out.println(bucket.name());
        }
    }
}