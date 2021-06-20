/*
 *  Copyright 2021 Netflix, Inc.
 *  <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.tasks.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import com.netflix.conductor.contribs.tasks.http.HttpTask.Input;
import com.netflix.conductor.contribs.tasks.http.HttpTask.HttpResponse;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_HTTP;

/**
 * Task that enables calling another HTTP endpoint as part of its execution
 */
@Component(TASK_TYPE_HTTP)
public class NonBlockingHttpTask extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingHttpTask.class);

    public static final String REQUEST_PARAMETER_NAME = "http_request";

    static final String MISSING_REQUEST = "Missing HTTP request. Task input MUST have a '" + REQUEST_PARAMETER_NAME
            + "' key with HttpTask.Input as value. See documentation for HttpTask for required input parameters";

    private final TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>() {
    };
    private final TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>() {
    };
    protected ObjectMapper objectMapper;
    protected RestTemplateProvider restTemplateProvider;
    private final String requestParameter;

    @Autowired
    public NonBlockingHttpTask(RestTemplateProvider restTemplateProvider,
                               ObjectMapper objectMapper) {
        this(TASK_TYPE_HTTP, restTemplateProvider, objectMapper);

    }

    public NonBlockingHttpTask(String name,
                               RestTemplateProvider restTemplateProvider,
                               ObjectMapper objectMapper) {
        super(name);
        this.restTemplateProvider = restTemplateProvider;
        this.objectMapper = objectMapper;
        this.requestParameter = REQUEST_PARAMETER_NAME;
        LOGGER.info("{} initialized...", getTaskType());
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
        Object request = task.getInputData().get(requestParameter);
        task.setWorkerId(Utils.getServerId());
        if (request == null) {
            task.setReasonForIncompletion(MISSING_REQUEST);
            task.setStatus(Status.FAILED);
            return;
        }

        HttpTask.Input input = objectMapper.convertValue(request, HttpTask.Input.class);
        if (input.getUri() == null) {
            String reason = "Missing HTTP URI.  See documentation for HttpTask for required input parameters";
            task.setReasonForIncompletion(reason);
            task.setStatus(Status.FAILED);
            return;
        }

        if (input.getMethod() == null) {
            String reason = "No HTTP method specified";
            task.setReasonForIncompletion(reason);
            task.setStatus(Status.FAILED);
            return;
        }

        try {

            //asyncHttpCall(input, task, executor);
            asyncHttpCall2(input, task);

        } catch (Exception e) {
            LOGGER.error("Failed to invoke {} task: {} - uri: {}, vipAddress: {} in workflow: {}", getTaskType(), task.getTaskId(),
                    input.getUri(), input.getVipAddress(), task.getWorkflowInstanceId(), e);
            task.setStatus(Status.FAILED);
            task.setReasonForIncompletion("Failed to invoke " + getTaskType() + " task due to: " + e);
            task.getOutputData().put("response", e.toString());
        }
    }


    protected void asyncHttpCall(Input input, Task task, WorkflowExecutor executor) throws Exception {


        WebClient.RequestBodySpec client = WebClient
                .create(input.getUri())
                .method(input.getMethod())
                .accept(MediaType.valueOf(input.getAccept()))
                .contentType(MediaType.valueOf(input.getContentType()));

        input.headers.forEach((key, value) -> client.header(key, value.toString()));
        Mono<ClientResponse> cr = null;
        if (input.getBody() != null) {
            cr = client.bodyValue(input.getBody()).exchange();
        } else {
            cr = client.exchange();
        }

        cr.subscribe(clientResponse -> {
            HttpResponse response = new HttpResponse();
            response.headers = clientResponse.headers().asHttpHeaders();
            response.reasonPhrase = clientResponse.statusCode().getReasonPhrase();
            response.statusCode = clientResponse.rawStatusCode();
            Mono<String> bodyMono = clientResponse.bodyToMono(String.class);
            LOGGER.debug("Response: {}, task:{}", response.statusCode, task.getTaskId());

            if (!clientResponse.statusCode().isError()) {
                if (isAsyncComplete(task)) {
                    task.setStatus(Status.IN_PROGRESS);
                } else {
                    task.setStatus(Status.COMPLETED);
                }
            } else {
                task.setStatus(Status.FAILED);
            }

            bodyMono.subscribe(responseBody -> {
                response.body = extractBody(responseBody);

                if (!clientResponse.statusCode().isError()) {
                    task.getOutputData().put("response", response.asMap());
                }
                if (response.body != null && clientResponse.statusCode().isError()) {
                    task.setReasonForIncompletion(response.body.toString());
                } else {
                    task.setReasonForIncompletion("No response from the remote service");
                }
                LOGGER.info("Updating task with status {} and result {}", task.getStatus(), task.getOutputData().keySet());
                executor.updateTask(new TaskResult(task));
            });

        });

        task.setStatus(Status.IN_PROGRESS);
    }

    protected void asyncHttpCall2(Input input, Task task) throws Exception {


        WebClient.RequestBodySpec client = WebClient
                .create(input.getUri())
                .method(input.getMethod())
                .accept(MediaType.valueOf(input.getAccept()))
                .contentType(MediaType.valueOf(input.getContentType()));

        input.headers.forEach((key, value) -> client.header(key, value.toString()));
        Mono<ClientResponse> cr = null;
        if (input.getBody() != null) {
            cr = client.bodyValue(input.getBody()).exchange();
        } else {
            cr = client.exchange();
        }



        ClientResponse clientResponse = cr.block();
        HttpResponse response = new HttpResponse();
        response.headers = clientResponse.headers().asHttpHeaders();
        response.reasonPhrase = clientResponse.statusCode().getReasonPhrase();
        response.statusCode = clientResponse.rawStatusCode();
        Mono<String> bodyMono = clientResponse.bodyToMono(String.class);

        LOGGER.debug("Response: {}, task:{}", response.statusCode, task.getTaskId());

        if (!clientResponse.statusCode().isError()) {
            if (isAsyncComplete(task)) {
                task.setStatus(Status.IN_PROGRESS);
            } else {
                task.setStatus(Status.COMPLETED);
            }
        } else {
            task.setStatus(Status.FAILED);
        }
        String responseBody = bodyMono.block();
        response.body = extractBody(responseBody);
        if (!clientResponse.statusCode().isError()) {
            task.getOutputData().put("response", response.asMap());
        }
        if (response.body != null && clientResponse.statusCode().isError()) {
            task.setReasonForIncompletion(response.body.toString());
        } else {
            task.setReasonForIncompletion("No response from the remote service");
        }


        //bodyMono.block();
    }


    private Object extractBody(String responseBody) {
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
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
        return false;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
        task.setStatus(Status.CANCELED);
    }

    @Override
    public boolean isAsync() {
        return true;
    }

}
