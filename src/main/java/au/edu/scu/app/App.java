package au.edu.scu.app;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
// snippet-end:[s3.java2.s3_bucket_ops.import]
// snippet-start:[s3.java2.s3_bucket_ops.main]

/**
 * Before running this Java V2 code example, set up your development environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class App {

    public static void main(String[] args) {

        // snippet-start:[s3.java2.s3_bucket_ops.region]
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        String myBucket = "yihangzha-a1-bucket";
        Map<String, Integer> tags = countObjects(s3, "yzha");
        String html = convertToHtml(tags);
        createObject(s3, myBucket, "home.html", html);
    }

    public static Map<String,Integer> countObjects(S3Client s3, String bucketName){
        Map<String, Integer> objectMap = new HashMap<>();
        //initiate the list of objects
        objectMap.put("Web", 0);
        objectMap.put("Text", 0);
        objectMap.put("Image", 0);
        objectMap.put("Excel", 0);
        objectMap.put("Other", 0);
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
                if(myValue.key().endsWith(".html")) {
                    objectMap.put("Web", objectMap.get("Web") + 1);
                } else if(myValue.key().endsWith(".txt")) {
                    objectMap.put("Text", objectMap.get("Text") + 1);
                } else if(myValue.key().endsWith(".jpg")) {
                    objectMap.put("Image", objectMap.get("Image") + 1);
                } else if(myValue.key().endsWith(".xlsx") || myValue.key().endsWith(".xls")) {
                    objectMap.put("Excel", objectMap.get("Excel") + 1);
                }else {
                    objectMap.put("Other", objectMap.get("Other") + 1);
                }
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return objectMap;
    }

    public static String convertToHtml(Map<String,Integer> objectMap){
        // convert the map to html
        String html = "<html><body><h1>File Type and Counts</h1><ul>";
        try {
            Set<String> keys = objectMap.keySet();
            for(String key: keys) {
                html += "<li>" + key + ": " + objectMap.get(key) + "</li>";
            }
            html += "</ul></body></html>";
            return html;
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return html;
    }

    public static void createObject(S3Client s3, String bucketName,String filename, String html) {
        // upload the html file to the bucket
        try {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(filename)
                    .contentType("text/html")
                    .build();
            byte[] bytes = html.getBytes();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            s3.putObject(objectRequest, RequestBody.fromByteBuffer(buffer));
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // snippet-end:[s3.java2.s3_bucket_ops.main]
}