package com.raft.lambda.api;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.raft.lambda.model.Product;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class ProductLambdaHandler implements RequestStreamHandler {

    private final String DYNAMO_TABLE = "Products";
    private final AmazonDynamoDB client;
    private final DynamoDB dynamoDB;
    private final Table productsTable;

    public ProductLambdaHandler() {
        client = AmazonDynamoDBClientBuilder.defaultClient();
        dynamoDB = new DynamoDB(client);
        productsTable = dynamoDB.getTable(DYNAMO_TABLE);
    }

    /**
     * Get product by id (path parameter or query string parameter)
     * @param inputStream .
     * @param outputStream .
     * @param context .
     * @throws IOException .
     */
    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser parser = new JSONParser();
        JSONObject responseObject = new JSONObject();
        JSONObject responseBody = new JSONObject();

        int id;
        Item resItem = null;
        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);

            // path parameters
            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");
                if (pps.get("id") != null) {
                    id = Integer.parseInt((String) pps.get("id"));
                    resItem = productsTable.getItem("id", id);
                }
            }
            // query string parameters
            else if (reqObject.get("queryStringParameters") != null) {
                JSONObject qps = (JSONObject) reqObject.get("queryStringParameters");
                if (qps.get("id") != null) {
                    id = Integer.parseInt((String) qps.get("id"));
                    resItem = productsTable.getItem("id", id);
                }
            }

            if (resItem != null) {
                Product product = new Product(resItem.toJSON());
                responseBody.put("product", product);
                responseBody.put("statusCode", 200);
            } else {
                responseBody.put("message", "No items found");
                responseBody.put("statusCode", 404);
            }

            responseObject.put("body", responseBody.toString());
        } catch (ParseException e) {
            context.getLogger().log("ERROR: " + e.getMessage());
        }

        writer.write(responseObject.toJSONString());
        reader.close();
        writer.close();
    }

    /**
     * Get all products
     * @param inputStream .
     * @param outputStream .
     * @param context .
     * @throws IOException .
     */
    public void handleGetAllRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser parser = new JSONParser();
        JSONObject responseObject = new JSONObject();
        JSONObject responseBody = new JSONObject();

        ItemCollection<ScanOutcome> productItems = productsTable.scan();
        Iterator<Item> itemIterator = productItems.iterator();
        List<Product> products = new ArrayList<>();
        while (itemIterator.hasNext()) {
            Product product = new Product(itemIterator.next().toJSON());
            products.add(product);
        }

        responseBody.put("products", products);
        responseBody.put("statusCode", 200);
        responseObject.put("body", responseBody.toString());

        writer.write(responseObject.toJSONString());
        reader.close();
        writer.close();
    }

    /**
     * Create or update product.
     * @param inputStream .
     * @param outputStream .
     * @param context .
     * @throws IOException .
     */
    public void handlePutRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser parser = new JSONParser();
        JSONObject responseObject = new JSONObject();
        JSONObject responseBody = new JSONObject();

        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);

            if (reqObject.get("body") != null) {
                Product product = new Product((String) reqObject.get("body"));

                productsTable
                        .putItem(new PutItemSpec().withItem(
                                        new Item()
                                                .withNumber("id", product.getId())
                                                .withString("name", product.getName())
                                                .withNumber("price", product.getPrice())
                                )
                        );
                responseBody.put("message", "New Item created/updated");
                responseObject.put("statusCode", 200);
                responseObject.put("body", responseBody.toString());
            }
        } catch (ParseException e) {
            context.getLogger().log("ERROR: " + e.getMessage());
        }

        writer.write(responseObject.toJSONString());
        reader.close();
        writer.close();
    }

    /**
     * Delete item by id (path parameter)
     * @param inputStream .
     * @param outputStream .
     * @param context .
     * @throws IOException .
     */
    public void handleDeleteRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser parser = new JSONParser();
        JSONObject responseObject = new JSONObject();
        JSONObject responseBody = new JSONObject();

        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);

            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");
                if (pps.get("id") != null) {
                    int id = Integer.parseInt((String) pps.get("id"));
                    productsTable.deleteItem("id", id);
                }
            }

            responseBody.put("message", "Item deleted");
            responseObject.put("statusCode", 201);
            responseObject.put("body", responseBody.toString());
        } catch (ParseException e) {
            responseBody.put("message", "No items found");
            responseBody.put("statusCode", 404);
        }
        writer.write(responseObject.toJSONString());
        reader.close();
        writer.close();
    }
}
