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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.contribs.aws.AWSClientConfiguration;
import com.netflix.conductor.core.events.DefaultEventQueueManager;
import com.netflix.conductor.core.events.queue.DefaultEventQueueProcessor;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Monitors and processes messages on the default event queues that Conductor listens on.
 * <p>
 * The default event queue type is controlled using the property: <code>conductor.default-event-queue.type</code>
 * This queue handler does not use event handlers
 */
@Component
@ConditionalOnProperty(name = "conductor.sqs-event-queue-processor.enabled", havingValue = "true", matchIfMissing = true)
public class SQSEventQueueProcessor extends DefaultEventQueueProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSEventQueueProcessor.class);


    private Map<Status, ObservableQueue> defaultEventQueues;

    private Map<String, Map<Status, ObservableQueue>> queuesMap;

    private DefaultEventQueueManager defaultEventQueueManager;

    public SQSEventQueueProcessor(Map<String, Map<Status, ObservableQueue>> queuesMap,
                                  DefaultEventQueueManager defaultEventQueueManager,
                                  ExecutionService executionService,
                                  ObjectMapper objectMapper,
                                  QueueDAO queueDAO) {

        super(new HashMap<>(), executionService, objectMapper);
        this.queuesMap = queuesMap;
        this.defaultEventQueueManager = defaultEventQueueManager;
        queuesMap.entrySet().forEach(entry -> {
            String region = entry.getKey();
            Map<Status, ObservableQueue> queues = entry.getValue();
            SQSEventQueueExecutor processor = new SQSEventQueueExecutor(queues, executionService, objectMapper, queueDAO);
            if (region.equals(AWSClientConfiguration.DEFAULT_REGION_KEY)) {
                defaultEventQueues = queues;
            }

            LOGGER.info("SQSEventQueueProcessor configured for region {} with queues {}",
                    region, queues.values().stream().map(ObservableQueue::getURI).collect(Collectors.toList()));
        });
    }

    @Override
    public Map<String, Long> size() {
        Map<String, Long> queues = new HashMap<>();
        queuesMap.entrySet().forEach(e -> {
            String region = e.getKey();
            e.getValue().entrySet().forEach(q -> {
                Status status = q.getKey();
                ObservableQueue queue  = q.getValue();
                Long size = queue.size();
                queues.put(queue.getURI(), size);
            });
        });

        defaultEventQueueManager.getQueueSizes().entrySet().forEach(e -> {
            String queueType = e.getKey();
            Map<String, Long> queueDetail = e.getValue();
            queueDetail.entrySet().forEach(qe -> {
                String key = qe.getKey();
                queues.put(key, qe.getValue());
            });
        });


        return queues;
    }

    @Override
    public Map<Status, String> queues() {
        Map<Status, String> queues = new HashMap<>();
        queuesMap.entrySet().forEach(e -> {
            String region = e.getKey();
            e.getValue().entrySet().forEach(q -> {
                Status status = q.getKey();
                ObservableQueue queue  = q.getValue();
                queues.compute(status, (key, value) -> {
                    return (value == null) ? queue.getURI() : value + "," + queue.getURI();
                });
            });
        });
        return queues;
    }

    @Override
    public void updateByTaskRefName(String workflowId, String taskRefName, Map<String, Object> output, Status status) throws Exception {
        super.updateByTaskRefName(workflowId, taskRefName, output, status);
    }

    @Override
    public void updateByTaskId(String workflowId, String taskId, Map<String, Object> output, Status status) throws Exception {
        super.updateByTaskId(workflowId, taskId, output, status);
    }
}
