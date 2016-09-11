package io.advantageous.reakt.dynamo;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.*;
import io.advantageous.reakt.promise.Promise;
import io.advantageous.reakt.promise.Promises;

import java.util.List;
import java.util.Map;

public class DynamoReaktClient {


    private final AmazonDynamoDBAsyncClient dynamoDBAsyncClient;

    public DynamoReaktClient(AmazonDynamoDBAsyncClient dynamoDBAsyncClient) {
        this.dynamoDBAsyncClient = dynamoDBAsyncClient;
    }

    public static DynamoReaktClient create(final AmazonDynamoDBAsyncClient dynamoDBAsyncClient) {
        return new DynamoReaktClient(dynamoDBAsyncClient);

    }


    /**
     * Simplified method form for invoking the ListTables operation.
     */
    public Promise<ListTablesResult> listTables() {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Simplified method form for invoking the ListTables operation.
     *
     * @param exclusiveStartTableName exclusiveStartTableName
     * @return async promise
     */
    public Promise<ListTablesResult> listTables(final String exclusiveStartTableName) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(exclusiveStartTableName, convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Simplified method form for invoking the ListTables operation.
     *
     * @param exclusiveStartTableName exclusiveStartTableName
     * @param limit                   limit
     * @return async promise
     */
    public Promise<ListTablesResult> listTables(final String exclusiveStartTableName, final int limit) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(exclusiveStartTableName, convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Simplified method form for invoking the ListTables operation.
     *
     * @param limit limit
     * @return async promise
     */
    public Promise<ListTablesResult> listTables(final int limit) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(limit, convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Returns an array of table names associated with the current account and endpoint.
     *
     * @param listTablesRequest listTablesRequest
     * @return async promise
     */
    public Promise<ListTablesResult> listTables(ListTablesRequest listTablesRequest) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(listTablesRequest, convertPromiseToAsyncResult(returnPromise)));
    }


    private <REQUEST extends AmazonWebServiceRequest, RESPONSE> AsyncHandler<REQUEST, RESPONSE>
    convertPromiseToAsyncResult(final Promise<RESPONSE> returnPromise) {
        return new AsyncHandler<REQUEST, RESPONSE>() {
            @Override
            public void onError(Exception exception) {
                returnPromise.reject(exception);
            }

            @Override
            public void onSuccess(REQUEST request, RESPONSE response) {
                returnPromise.resolve(response);
            }
        };
    }


    /**
     * The BatchGetItem operation returns the attributes of one or more items from one or more tables.
     *
     * @param request BatchGetItemRequest
     * @return Promise of BatchGetItemResult
     */
    public Promise<BatchGetItemResult> batchGetItem(final BatchGetItemRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchGetItemAsync(request, convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * The BatchGetItem operation returns the attributes of one or more items from one or more tables.
     *
     * @param requestItems Map of table names and keys and attributes.
     * @return Promise of BatchGetItemResult
     */
    public Promise<BatchGetItemResult> batchGetItem(final Map<String, KeysAndAttributes> requestItems) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchGetItemAsync(requestItems, convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * The BatchGetItem operation returns the attributes of one or more items from one or more tables.
     *
     * @param requestItems           Map of table names and keys and attributes.
     * @param returnConsumedCapacity returnConsumedCapacity
     * @return Promise of BatchGetItemResult
     */
    public Promise<BatchGetItemResult> batchGetItem(final Map<String, KeysAndAttributes> requestItems,
                                                    final String returnConsumedCapacity) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchGetItemAsync(requestItems, returnConsumedCapacity,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * batchWriteItemAsync
     *
     * @param request request
     * @return promise of BatchWriteItemResult
     */
    public Promise<BatchWriteItemResult> batchWriteItem(final BatchWriteItemRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchWriteItemAsync(request,
                        convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * batchWriteItemAsync
     *
     * @param requestItems requestItems
     * @return promise of BatchWriteItemResult
     */
    public Promise<BatchWriteItemResult> batchWriteItem(final Map<String, List<WriteRequest>> requestItems) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchWriteItemAsync(requestItems,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Create table
     *
     * @param request create table request
     * @return promise of create table
     */
    public Promise<CreateTableResult> createTable(final CreateTableRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.createTableAsync(request,
                        convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * @param attributeDefinitions  attributeDefinitions
     * @param tableName             table name
     * @param keySchema             key schema
     * @param provisionedThroughput provisionedThroughput
     * @return promise of CreateTableResult
     */
    public Promise<CreateTableResult> createTable(final List<AttributeDefinition> attributeDefinitions,
                                                  final String tableName,
                                                  final List<KeySchemaElement> keySchema,
                                                  final ProvisionedThroughput provisionedThroughput) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.createTableAsync(attributeDefinitions, tableName, keySchema, provisionedThroughput,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /*
     Future<DeleteItemResult>	deleteItemAsync(String tableName, Map<String,AttributeValue> key)
     Simplified method form for invoking the DeleteItem operation.
     Future<DeleteItemResult>	deleteItemAsync(String tableName, Map<String,AttributeValue> key, AsyncHandler<DeleteItemRequest,DeleteItemResult> asyncHandler)
     Simplified method form for invoking the DeleteItem operation with an AsyncHandler.
     Future<DeleteItemResult>	deleteItemAsync(String tableName, Map<String,AttributeValue> key, String returnValues)
     Simplified method form for invoking the DeleteItem operation.
     Future<DeleteItemResult>	deleteItemAsync(String tableName, Map<String,AttributeValue> key, String returnValues, AsyncHandler<DeleteItemRequest,DeleteItemResult> asyncHandler)
     Simplified method form for invoking the DeleteItem operation with an AsyncHandler.
     */


    /**
     * Deletes a single item in a table by primary key.
     *
     * @param request delete request
     * @return promise of DeleteItemResult
     */
    public Promise<DeleteItemResult> deleteItem(final DeleteItemRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.deleteItemAsync(request,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Simplified method form for invoking the DeleteItem operation.
     *
     * @param tableName table name
     * @param key       key
     * @return promise of DeleteItemResult
     */
    public Promise<DeleteItemResult> deleteItem(final String tableName, final Map<String, AttributeValue> key) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.deleteItemAsync(tableName, key,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Simplified method form for invoking the DeleteItem operation.
     *
     * @param tableName   table name
     * @param key         key
     * @param returnItems return items
     * @return promise of DeleteItemResult
     */
    public Promise<DeleteItemResult> deleteItem(final String tableName,
                                                final Map<String, AttributeValue> key,
                                                final String returnItems) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.deleteItemAsync(tableName, key,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * The DeleteTable operation deletes a table and all of its items.
     *
     * @param request request
     * @return promise of DeleteTableResult
     */
    public Promise<DeleteTableResult> deleteTable(final DeleteTableRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.deleteTableAsync(request,
                        convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Simplified method form for invoking the DeleteTable operation.
     *
     * @param tableName tableName
     * @return promise of DeleteTableResult
     */
    public Promise<DeleteTableResult> deleteTable(final String tableName) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.deleteTableAsync(tableName,
                        convertPromiseToAsyncResult(returnPromise)));
    }
}
