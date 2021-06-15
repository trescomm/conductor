package com.netflix.conductor.contribs.tasks.aws;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.aws.AWSClientConfiguration;
import com.netflix.conductor.contribs.secrets.SecretsManager;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.dao.QueueDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_AWS_LAMBDA;

/**
 * Task that enables calling AWS Lambda function
 */
@Component(TASK_TYPE_AWS_LAMBDA)
@ConditionalOnProperty(name = "conductor.aws-lambda.task.enabled", havingValue = "true")
public class AWSLambdaTask extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSLambdaTask.class);

    public static final String FUNCTION_NAME_PARAMETER = "lambda_function_name";

    public static final String ASSUME_ROLE_PARAMETER = "assumeRole";

    public static final String EXTERNAL_ID_SECRET_NAME = "externalIdSecretName";

    public static final String EXTERNAL_ID_SECRET_KEY = "externalIdSecretKey";

    public static final String MONITOR_QUEUE_NAME = "lambda_function_monitor_queue";

    private AWSClientConfiguration awsClientConfiguration;

    private SecretsManager secretsManager;

    private ObjectMapper objectMapper;

    private QueueDAO queueDAO;

    private final TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>() {
    };
    private final TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>() {
    };

    public AWSLambdaTask(AWSClientConfiguration awsClientConfiguration, SecretsManager secretsManager,
                         ObjectMapper objectMapper, QueueDAO queueDAO) {
        super(TASK_TYPE_AWS_LAMBDA);
        this.awsClientConfiguration = awsClientConfiguration;
        this.secretsManager = secretsManager;
        LOGGER.info("Using {} as secretsManager", secretsManager);
        this.objectMapper = objectMapper;
        this.queueDAO = queueDAO;
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        Object functionNameParam = task.getInputData().get(FUNCTION_NAME_PARAMETER);
        if (functionNameParam == null) {
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion("Missing function name in the input parameter.  " +
                    "Expected parameter by name: " + FUNCTION_NAME_PARAMETER);
            return;
        }
        String lambdaFunctionName = functionNameParam.toString();

        try {

            Object assumeRoleObj = task.getInputData().get(ASSUME_ROLE_PARAMETER);
            String roleArn = null;
            if (assumeRoleObj != null) {
                roleArn = assumeRoleObj.toString();
            }
            String externalId = null;

            if (roleArn != null) {
                String secretKey = roleArn + ":externalId";
                Object secretNameObj = task.getInputData().get(EXTERNAL_ID_SECRET_NAME);
                String secretName = null;
                if (secretNameObj != null) {
                    secretName = secretNameObj.toString();
                }
                Object secretKeyObj = task.getInputData().get(EXTERNAL_ID_SECRET_KEY);
                if (secretKeyObj != null) {
                    secretKey = secretKeyObj.toString();
                }

                if (!Strings.isNullOrEmpty(secretName) && !Strings.isNullOrEmpty(secretKey)) {
                    externalId = secretsManager.getSecret(secretName, secretKey);
                }
            }


            boolean isAsync = isAsyncComplete(task);
            Map<String, Object> response = execute(lambdaFunctionName, roleArn, externalId, task.getInputData(), isAsync);
            String requestId = response.get("requestId").toString();

            task.getOutputData().putAll(response);
            task.setStatus(isAsync ? Task.Status.IN_PROGRESS : Task.Status.COMPLETED);
            workflow.setCorrelationId(requestId);

            String payload = task.getTaskId();
            Message msg = new Message(requestId, payload, null);
            queueDAO.push(MONITOR_QUEUE_NAME, Arrays.asList(msg));

        } catch (Exception e) {
            LOGGER.error("Failed to execute lambda function {} with error {}", lambdaFunctionName, e.getMessage(), e);
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
        }
    }

    protected Map<String, Object> execute(String lambdaFunctionName, String roleArn, String externalId,
                                          Map<String, Object> input, boolean async) throws Exception {
        String payload = objectMapper.writeValueAsString(input);

        InvokeRequest request = new InvokeRequest()
                .withFunctionName(lambdaFunctionName)
                .withInvocationType(async ? InvocationType.Event : InvocationType.RequestResponse)
                .withLogType(LogType.Tail)
                .withPayload(payload);

        AWSLambda awsLambda = awsClientConfiguration.getLambdaClient(lambdaFunctionName, roleArn, externalId);
        InvokeResult result = awsLambda.invoke(request);
        String requestId = result.getSdkResponseMetadata().getRequestId();

        Integer statusCode = result.getStatusCode();
        ByteBuffer resultPayload = result.getPayload();

        if (!isSuccess(statusCode)) {
            throw new Exception(result.getFunctionError());
        }

        Map<String, Object> response = new HashMap<>();
        response.put("requestId", requestId);
        Object responseBody = extractBody(resultPayload);
        response.put("response", responseBody);

        return response;
    }

    private boolean isSuccess(Integer statusCode) {
        return statusCode != null && statusCode > 199 && statusCode < 300;
    }

    private Object extractBody(ByteBuffer responseBytes) {
        String responseBody = new String(responseBytes.array());

        try {
            JsonNode node = objectMapper.readTree(responseBody);
            if (node.isArray()) {
                return objectMapper.convertValue(node, listOfObj);
            } else if (node.isObject()) {
                return objectMapper.convertValue(node, mapOfObj);
            } else if (node.isNumber()) {
                return objectMapper.convertValue(node, Double.class);
            } else {
                return node.asText();
            }
        } catch (IOException jpe) {
            LOGGER.error("Error extracting response body", jpe);
            return responseBody;
        }
    }


    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        return false;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        return;
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Override
    public boolean isAsyncComplete(Task task) {
        return super.isAsyncComplete(task);
    }

}
