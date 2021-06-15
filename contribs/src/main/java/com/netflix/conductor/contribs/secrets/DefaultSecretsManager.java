package com.netflix.conductor.contribs.secrets;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "conductor.secrets.manager", havingValue = "default", matchIfMissing = true)
public class DefaultSecretsManager implements SecretsManager {

    @Override
    public String getSecret(String secretName, String secretKey) {
        return null;
    }
}
