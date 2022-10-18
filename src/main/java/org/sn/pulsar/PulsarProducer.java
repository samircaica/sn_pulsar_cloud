package org.sn.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.MalformedURLException;
import java.net.URL;

public class PulsarProducer {
    public static void main(String[] args) throws MalformedURLException, PulsarClientException {
        final String HOME = "/Users/scaica/sn";
        final String TOPIC = "persistent://tenant1/namespace1/my-topic";
        final String PULSAR_URL = "pulsar+ssl://free.o-0kdpn.snio.cloud:6651";

        URL issuerUrl = new URL("https://auth.streamnative.cloud/");
        URL credentialsUrl = new URL("file://"+HOME+"/o-0kdpn-free.json");
        String audience = "urn:sn:pulsar:o-0kdpn:free";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(PULSAR_URL)
                .authentication(AuthenticationFactoryOAuth2.clientCredentials(issuerUrl, credentialsUrl, audience))
                .build();

        Producer<byte[]> producer = client.newProducer().topic(TOPIC).create();

        for(int idx = 0; idx < 100; idx++) {
            String message = "Added message #"+idx;
            producer.send(message.getBytes());
        }

        System.exit(0);
    }
}
