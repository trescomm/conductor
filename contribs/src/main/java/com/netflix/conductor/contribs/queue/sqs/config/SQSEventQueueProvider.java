/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.sqs.config;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.netflix.conductor.contribs.aws.AWSClientConfiguration;
import com.netflix.conductor.contribs.queue.sqs.SQSObservableQueue;
import com.netflix.conductor.contribs.queue.sqs.SQSObservableQueue.Builder;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import rx.Scheduler;
import software.amazon.awssdk.arns.Arn;

public class SQSEventQueueProvider implements EventQueueProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSEventQueueProvider.class);
    private final Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();
    private final Map<String, AmazonSQS> sqsClients;
    private final int batchSize;
    private final long pollTimeInMS;
    private final int visibilityTimeoutInSeconds;
    private final Scheduler scheduler;

    public SQSEventQueueProvider(Map<String, AmazonSQS> sqsClients, SQSEventQueueProperties properties, Scheduler scheduler) {
        this.sqsClients = sqsClients;
        this.batchSize = properties.getBatchSize();
        this.pollTimeInMS = properties.getPollTimeDuration().toMillis();
        this.visibilityTimeoutInSeconds = (int) properties.getVisibilityTimeout().getSeconds();
        this.scheduler = scheduler;
    }

    @Override
    public String getQueueType() {
        return "sqs";
    }

    @Override
    @NonNull
    public ObservableQueue getQueue(String queueURI) {
        String region = AWSClientConfiguration.DEFAULT_REGION_KEY;
        try {
            Arn arn = Arn.fromString(queueURI);
            region = arn.region().orElse(AWSClientConfiguration.DEFAULT_REGION_KEY);
        }catch(IllegalArgumentException iae) {
            LOGGER.debug("queueURI {} is not arn, using default client", queueURI);
        }

        LOGGER.info("region: {}, clients: {}", region, sqsClients);

        AmazonSQS client = sqsClients.get(region);

        return queues.computeIfAbsent(queueURI, q -> new Builder()
            .withBatchSize(this.batchSize)
            .withClient(client)
            .withPollTimeInMS(this.pollTimeInMS)
            .withQueueName(queueURI)
            .withVisibilityTimeout(this.visibilityTimeoutInSeconds)
            .withScheduler(scheduler)
            .build());
    }
}
