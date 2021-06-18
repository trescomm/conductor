package com.netflix.conductor.contribs.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.netflix.conductor.contribs.queue.sqs.config.SQSEventQueueProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.arns.Arn;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "conductor.event-queues.sqs.enabled", havingValue = "true")
public class AWSClientConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSClientConfiguration.class);

    private AWSCredentialsProvider awsCredentialsProvider;

    private Map<String, AWSLambda> lambdaClientMap;

    private Map<String, LambdaClientWithExpiry> roleSpecificLambdaClientMap;

    private Map<String, AmazonSQS> sqsClientMap;

    public static final String DEFAULT_REGION_KEY = "__default__";

    public static class LambdaClientWithExpiry {

        private Date expiry;
        private AWSLambda lambdaClient;

        public LambdaClientWithExpiry(Date expiry, AWSLambda lambdaClient) {
            this.expiry = expiry;
            this.lambdaClient = lambdaClient;
        }
    }


    public AWSClientConfiguration(SQSEventQueueProperties sqsEventQueueProperties) {
        this.awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        this.lambdaClientMap = new ConcurrentHashMap<>();
        this.sqsClientMap = new HashMap<>();
        this.roleSpecificLambdaClientMap = new ConcurrentHashMap<>();

        List<String> queueRegions = sqsEventQueueProperties.getRegions();

        AmazonSQS defaultSQSClient = AmazonSQSClientBuilder
                .standard()
                .withCredentials(awsCredentialsProvider)
                .build();
        sqsClientMap.put(DEFAULT_REGION_KEY, defaultSQSClient);

        if (queueRegions != null || !queueRegions.isEmpty()) {
            for (String queueRegion : queueRegions) {
                String region = queueRegion.trim();
                if (region.isEmpty())
                    continue;
                AmazonSQS sqsClient = AmazonSQSClientBuilder
                        .standard()
                        .withCredentials(awsCredentialsProvider)
                        .withRegion(region)
                        .build();
                sqsClientMap.put(region, sqsClient);
            }
        }

    }

    @Bean
    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return awsCredentialsProvider;
    }

    public AWSLambda getLambdaClient(String functionArn, String roleARN, String externalId) {

        Arn arn = null;

        try {
            arn = Arn.fromString(functionArn);
        } catch (IllegalArgumentException iae) {
            LOGGER.debug("functionName {} is not a valid arn name, using defaults to try and execute the function in the same region");
        }

        String region = arn != null ? arn.region().orElse(null) : null;
        LOGGER.info("function  {} is in region {}", functionArn, region);
        String regionKey = region == null ? DEFAULT_REGION_KEY : region;

        if(!Strings.isNullOrEmpty(roleARN)) {

            String key = functionArn + "-" + roleARN;

            LambdaClientWithExpiry lambdaClientWithExpiry = roleSpecificLambdaClientMap.computeIfAbsent(key, s -> {
                return getLambdaClientWithExpiry(roleARN, region, externalId);
            });

            if(lambdaClientWithExpiry.expiry.getTime() < System.currentTimeMillis() ) {
                //Access is expired.
                LOGGER.info("Role access expired.  Creating a new client");
                lambdaClientWithExpiry = getLambdaClientWithExpiry(roleARN, region, externalId);
            }

            return lambdaClientWithExpiry.lambdaClient;

        }

        return lambdaClientMap.computeIfAbsent(regionKey, s -> {
            LOGGER.info("Creating new lambda client for the region {}", regionKey);
            AWSLambda awsLambda = AWSLambdaClientBuilder
                    .standard()
                    .withCredentials(awsCredentialsProvider)
                    .withRegion(region)
                    .build();
            return awsLambda;
        });
    }

    @VisibleForTesting
    protected LambdaClientWithExpiry getLambdaClientWithExpiry(String roleARN, String region, String externalId) {

        LOGGER.info("Creating new lambda client for the region {} with role {}", region, roleARN);

        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .build();

        AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(roleARN)
                .withExternalId(externalId)
                .withRoleSessionName(UUID.randomUUID().toString());

        LOGGER.info("Assuming role {} as user {}", roleARN, stsClient.getCallerIdentity(new GetCallerIdentityRequest()).getArn());
        AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
        Credentials sessionCredentials = roleResponse.getCredentials();

        BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getSecretAccessKey(),
                sessionCredentials.getSessionToken());
        Date expires = sessionCredentials.getExpiration();

        AWSLambda awsLambda = AWSLambdaClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(region)
                .build();

        return new LambdaClientWithExpiry(expires, awsLambda);
    }

    @Bean
    public Map<String, AmazonSQS> getSQSClients() {
        return sqsClientMap;
    }
}
