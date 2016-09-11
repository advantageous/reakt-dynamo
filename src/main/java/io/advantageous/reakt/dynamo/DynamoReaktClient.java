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
    public Promise<ListTablesResult> listTablesAsync() {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Simplified method form for invoking the ListTables operation.
     *
     * @param exclusiveStartTableName exclusiveStartTableName
     * @return async promise
     */
    public Promise<ListTablesResult> listTablesAsync(final String exclusiveStartTableName) {
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
    public Promise<ListTablesResult> listTablesAsync(final String exclusiveStartTableName, final int limit) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(exclusiveStartTableName, convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * Simplified method form for invoking the ListTables operation.
     *
     * @param limit limit
     * @return async promise
     */
    public Promise<ListTablesResult> listTablesAsync(final int limit) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.listTablesAsync(limit, convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * Returns an array of table names associated with the current account and endpoint.
     *
     * @param listTablesRequest listTablesRequest
     * @return async promise
     */
    public Promise<ListTablesResult> listTablesAsync(ListTablesRequest listTablesRequest) {
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
    public Promise<BatchGetItemResult> batchGetItemAsync(final BatchGetItemRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchGetItemAsync(request, convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * The BatchGetItem operation returns the attributes of one or more items from one or more tables.
     *
     * @param requestItems Map of table names and keys and attributes.
     * @return Promise of BatchGetItemResult
     */
    public Promise<BatchGetItemResult> batchGetItemAsync(final Map<String, KeysAndAttributes> requestItems) {
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
    public Promise<BatchGetItemResult> batchGetItemAsync(final Map<String, KeysAndAttributes> requestItems,
                                                         final String returnConsumedCapacity) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchGetItemAsync(requestItems, returnConsumedCapacity,
                convertPromiseToAsyncResult(returnPromise)));
    }

    /**
     * batchWriteItemAsync
     * @param request request
     * @return promise of BatchWriteItemResult
     */
    public Promise<BatchWriteItemResult> batchWriteItemAsync(final BatchWriteItemRequest request) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchWriteItemAsync(request,
                        convertPromiseToAsyncResult(returnPromise)));
    }


    /**
     * batchWriteItemAsync
     * @param requestItems requestItems
     * @return promise of BatchWriteItemResult
     */
    public Promise<BatchWriteItemResult> batchWriteItemAsync(final Map<String,List<WriteRequest>> requestItems) {
        return Promises.invokablePromise(returnPromise ->
                dynamoDBAsyncClient.batchWriteItemAsync(requestItems,
                        convertPromiseToAsyncResult(returnPromise)));
    }
}
