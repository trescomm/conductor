package com.netflix.conductor.contribs.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.secrets.SecretsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Base64;

@Component
@ConditionalOnProperty(name = "conductor.secrets.manager", havingValue = "aws")
public class AWSSecretsManager implements SecretsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSSecretsManager.class);

    private final ObjectMapper objectMapper;

    public AWSSecretsManager(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String getSecret(String secretName, String secretKey) {
        SecretsManagerClient secretsManagerClient = SecretsManagerClient.create();
        GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();

        GetSecretValueResponse valueResponse = secretsManagerClient.getSecretValue(valueRequest);
        String secret = valueResponse.secretString();
        if(secret == null) {
            secret = new String(Base64.getDecoder().decode(valueResponse.secretBinary().asByteBuffer()).array());
        }
        try {
            JsonNode node = objectMapper.readTree(secret);
            JsonNode key = node.get(secretKey);
            if(key == null) {
                return null;
            }
            return key.asText();

        }catch (Exception e) {
            LOGGER.error("Error parsing secrets", e);
            throw new RuntimeException(e);
        }

    }

}
