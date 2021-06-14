package com.netflix.conductor.contribs.secrets;

/**
 * Security Manager interface to manage secrets
 */
public interface SecretsManager {

    public String getSecret(String secretName, String secretKey);

}
