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
package com.netflix.conductor.contribs.queue.sqs;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.aws.AWSClientConfiguration;
import com.netflix.conductor.contribs.tasks.aws.AWSLambdaTask;
import com.netflix.conductor.core.events.queue.DefaultEventQueueProcessor;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.exception.ApplicationException;
import com.netflix.conductor.core.exception.ApplicationException.Code;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;

/**
 * Monitors and processes events form SQS queues.  Used by Conductor to mark tasks completion.
 * This implmentation over the {@link DefaultEventQueueProcessor} supports being able to handle multi-region queues.
 * Note - This is not used by Event handlers.
 * <p>
 * The default event queue type is controlled using the property: <code>conductor.default-event-queue.type</code>
 *
 */
public class SQSEventQueueExecutor {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultEventQueueProcessor.class);

    protected static final TypeReference<Map<String, Object>> _mapType = new TypeReference<Map<String, Object>>() {
    };

    private QueueDAO queueDAO;

    private ObjectMapper objectMapper;

    private ExecutionService executionService;

    public SQSEventQueueExecutor(Map<Status, ObservableQueue> queues, ExecutionService executionService,
                                 ObjectMapper objectMapper, QueueDAO queueDAO) {

        queues.entrySet().forEach(e -> startMonitor(e.getKey(), e.getValue()));
        queues.values().forEach(queue -> queue.start());

        this.executionService = executionService;
        this.objectMapper = objectMapper;
        this.queueDAO = queueDAO;
    }

    private void startMonitor(Status status, ObservableQueue queue) {

        queue.observe().subscribe((Message msg) -> {

            try {

                process(status, queue, msg);

            } catch (ApplicationException e) {
                if (e.getCode().equals(Code.NOT_FOUND)) {
                    LOGGER.error("Workflow ID specified is not valid for this environment");
                    queue.ack(Collections.singletonList(msg));
                }
                LOGGER.error("Error processing message: {}", msg, e);
            } catch (Exception e) {
                LOGGER.error("Error processing message: {}", msg, e);
            }
        }, (Throwable t) -> LOGGER.error(t.getMessage(), t));

        LOGGER.info("QueueListener::STARTED...listening for " + queue.getName());
    }

    protected void process(Status status, ObservableQueue queue, Message msg) {

        try {

            LOGGER.debug("Got message with id: {} and payload: {}", msg.getId(), msg.getPayload());
            Map<String, Object> msgMap = objectMapper.readValue(msg.getPayload(), _mapType);
            Map<String, Object> requestContext = (Map<String, Object>) msgMap.get("requestContext");
            if (requestContext == null) {
                LOGGER.error("Bad message/SQS config?  Missing requestContext");
                queue.ack(Arrays.asList(msg));
                return;
            }
            String requestId = (String) requestContext.get("requestId");
            if (requestId == null) {
                LOGGER.error("Bad message/SQS config?  Missing requestId in requestContext");
                queue.ack(Arrays.asList(msg));
                return;
            }


            Message monitorQueueMsg = queueDAO.get(AWSLambdaTask.MONITOR_QUEUE_NAME, requestId);
            if(monitorQueueMsg == null) {
                //we could not find this request Id in the queue... what happened?
                LOGGER.error("Cannot find the message {} with requestId {} in the monitor queue {}", msg.getPayload(), requestId, queue.getURI());
                queue.ack(Arrays.asList(msg));
                return;
            }
            String monitorQueueMsTaskId = monitorQueueMsg.getPayload();
            Task task = executionService.getTask(monitorQueueMsTaskId);
            if(task == null) {
                LOGGER.error("No task found by Id: " + monitorQueueMsTaskId);
                queue.ack(Arrays.asList(msg));
                return;
            }

            if(task.getStatus().isTerminal()) {
                LOGGER.error("Task with id {} is already in the terminal status.  Ignoring the message", task.getTaskId());
                return;
            }

            task.getOutputData().putAll(msgMap);
            task.setStatus(status);
            executionService.updateTask(task);
            queueDAO.remove(AWSLambdaTask.MONITOR_QUEUE_NAME, requestId);
            queue.ack(Arrays.asList(msg));

        }catch (JsonProcessingException jpe) {
            LOGGER.error("Bad message.  Expecting a json payload with requestContext.requestId present", jpe);
            throw new RuntimeException(jpe);
        }
    }

}
