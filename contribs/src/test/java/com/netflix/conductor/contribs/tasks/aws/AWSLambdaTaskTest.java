package com.netflix.conductor.contribs.tasks.aws;

import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.aws.AWSClientConfiguration;
import com.netflix.conductor.contribs.secrets.SecretsManager;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.QueueDAO;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class AWSLambdaTaskTest {

    private static final Map<String, Object> ASYNC_RESPONSE = new HashMap<>();
    private static final Map<String, Object> SYNC_RESPONSE = new HashMap<>();
    private static final Map<String, Object> responseMap = new HashMap<>();

    static {
        responseMap.put("key", "value");
        responseMap.put("key_num", 123);

        ASYNC_RESPONSE.put("requestId", "request_id_123");

        SYNC_RESPONSE.put("requestId", "request_id_123");
        SYNC_RESPONSE.put("response", responseMap);

    }
    public static final String LAMBDA_FUNCTION_NAME = "lambdaFunctionName";

    private static AWSLambdaTask lambdaTask;

    private static WorkflowExecutor workflowExecutor;



    @BeforeClass
    public static void setup() throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, String> responseMetadata = new HashMap<>();
        responseMetadata.put("AWS_REQUEST_ID", "request_id_123");   //com.amazonaws.ResponseMetadata.metadata

        InvokeResult syncResult = new InvokeResult();
        syncResult.setStatusCode(200);
        syncResult.setSdkResponseMetadata(new ResponseMetadata(responseMetadata));

        syncResult.setPayload(ByteBuffer.wrap(objectMapper.writeValueAsString(responseMap).getBytes()));

        AWSClientConfiguration acc = mock(AWSClientConfiguration.class);
        AWSLambda awsLambda = mock(AWSLambda.class);
        when(awsLambda.invoke(any())).thenReturn(syncResult);
        when(acc.getLambdaClient(any(), any(), any())).thenReturn(awsLambda);

        SecretsManager sm = mock(SecretsManager.class);
        QueueDAO queueDAO = mock(QueueDAO.class);
        lambdaTask = new AWSLambdaTask(acc, sm, objectMapper, queueDAO);
        workflowExecutor = mock(WorkflowExecutor.class);
    }

    @Test
    public void testSyncExecution() throws Exception {

        Workflow workflow = new Workflow();
        Task task = new Task();
        task.setWorkflowTask(new WorkflowTask());
        task.getWorkflowTask().setAsyncComplete(false);
        Map<String, Object> input = new HashMap<>();
        task.getInputData().put(AWSLambdaTask.FUNCTION_NAME_PARAMETER, LAMBDA_FUNCTION_NAME);


        lambdaTask.start(workflow, task, workflowExecutor);
        assertEquals(Task.Status.COMPLETED, task.getStatus());
        Map<String, Object> output = task.getOutputData();
        assertNotNull(output);
        Object response = output.get("response");
        assertNotNull("Task Output does not contain response key.  Output=" + output, response);
        assertTrue("Response is not a map.  found " + response.getClass().getName(), Map.class.isAssignableFrom(response.getClass()));
        assertEquals(responseMap, response);
    }

    @Test
    public void testAsyncExecution() throws Exception {

        Workflow workflow = new Workflow();
        Task task = new Task();
        task.setWorkflowTask(new WorkflowTask());
        task.getWorkflowTask().setAsyncComplete(false);
        task.getInputData().put(AWSLambdaTask.FUNCTION_NAME_PARAMETER, LAMBDA_FUNCTION_NAME);


        lambdaTask.start(workflow, task, workflowExecutor);
        assertEquals(Task.Status.COMPLETED, task.getStatus());
        Map<String, Object> output = task.getOutputData();
        assertNotNull(output);
        assertNotNull("Missing requestId in output: " + output, output.get("requestId"));

        Object response = output.get("response");
        assertNotNull("Task Output does not contain response key.  Output=" + output, response);
        assertTrue("Response is not a map.  found " + response.getClass().getName(), Map.class.isAssignableFrom(response.getClass()));
        assertEquals(responseMap, response);
    }

}
