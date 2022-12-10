package au.edu.scu.app;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3Event;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.commons.io.IOUtils;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;
// import S3EventRecord;


/**
 * Before running this Java V2 code example, set up your development environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class App {

    public static void main(String[] args) {
        // Create an instance of the "handleRequest()" function
        System.out.println("Hello World!");
//        handleRequest(null, null);
    }


    public void handleRequest(S3EventNotification event, Context context) {
        // Get the bucket name and object key from the S3 event
        S3EventNotificationRecord record = event.getRecords().get(0);
        String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
        String objectKey = event.getRecords().get(0).getS3().getObject().getKey();
        context.getLogger().log("Received event for bucket: " + bucketName + ", key: " + objectKey);

        // Reading the content of the object
        AmazonS3Client s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
        S3Object s3Object = s3Client.getObject(bucketName, objectKey);
        try {
            // Get the object's input stream and convert it to a String
            InputStream content = s3Object.getObjectContent();
            String data = IOUtils.toString(content, "UTF-8");
            context.getLogger().log("Start to parse the data");
            // Parse the data
            parseJson(data, context);
            context.getLogger().log("Received event for bucket: " + bucketName + ", key: " + objectKey);
        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
        }

        // read the notification event
        List<S3EventNotificationRecord> records = event.getRecords();
        for (S3EventNotificationRecord r : records) {
            String eventName = r.getEventName();
        }
        // when the key is Student.json, the lambda function will be triggered
        //retrieve key name
        //parase the content of the file
        //store the data into the database
    }

    public void parseJson(String data, Context context) {
        // create a JSONArray from the string data.
        JSONArray jsonArray = new JSONArray(data);
        // iterate over the array and print each object.
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            context.getLogger().log("Find an item:"+jsonObject.toString());
            // create an Item object from the JSON object
            int id = getLatestId();
            // insert the item into the database
            insertData(id, jsonObject, context);
        }
    }

    public void insertData(int id, JSONObject jsonObject, Context context) {
        // create a DynamoDB client and table object
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("Student");
        Item item = new Item()
                .withPrimaryKey("Id", id)
                .withString("Name", jsonObject.getString("Name"))
                .withNumber("Age", jsonObject.getInt("Age"))
                .withString("Degree", jsonObject.getString("Degree"))
                .withNumber("Credits", jsonObject.getInt("Credits"));

        // insert the item into the DynamoDB table
        PutItemOutcome outcome = table.putItem(item);
        context.getLogger().log("Inserted item: " + outcome.getPutItemResult());
        //close the client
        client.shutdown();
    }



        public int getLatestId() {
            // scan on DynamoDB table student and get the max value of the Id
            // create a DynamoDB client and table object
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
            DynamoDB dynamoDB = new DynamoDB(client);
            Table table = dynamoDB.getTable("Student");

            // scan the table and get the maximum value of the "Id" attribute
            ScanSpec scanSpec = new ScanSpec()
                    .withProjectionExpression("#i")
                    .withFilterExpression("#i > :val")
                    .withNameMap(new NameMap().with("#i", "Id"))
                    .withValueMap(new ValueMap().withNumber(":val", 0))
                    .withMaxPageSize(1);
            // return 0 if the table is empty, otherwise return the maximum value of the "Id" attribute
            try {
                Item item = table.scan(scanSpec).iterator().next();
                return item.getInt("MAX(#i)")+1;
            } catch (NoSuchElementException e) {
                return 0;
            }
        }

}