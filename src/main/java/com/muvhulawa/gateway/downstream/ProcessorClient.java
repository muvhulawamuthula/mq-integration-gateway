package com.muvhulawa.gateway.downstream;

import com.muvhulawa.gateway.config.GatewayProperties;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * Forwards a pacs.008 payload to the payment processor over HTTP and classifies the result so the
 * caller can decide whether to acknowledge, retry, or dead-letter:
 *
 * <ul>
 *   <li>2xx -&gt; a {@link ProcessorResponse} (including business RJCT — a successful exchange).</li>
 *   <li>5xx or I/O failure -&gt; {@link TransientDownstreamException} (retryable).</li>
 *   <li>4xx -&gt; {@link PermanentDownstreamException} (poison — retrying won't help).</li>
 * </ul>
 */
@Component
public class ProcessorClient {

    private final RestClient restClient;
    private final String path;

    public ProcessorClient(RestClient.Builder builder, GatewayProperties properties) {
        this.restClient = builder.baseUrl(properties.getProcessor().getBaseUrl()).build();
        this.path = properties.getProcessor().getPath();
    }

    public ProcessorResponse submit(String pacs008Xml) {
        try {
            ResponseEntity<String> response = restClient.post()
                    .uri(path)
                    .contentType(MediaType.APPLICATION_XML)
                    .body(pacs008Xml)
                    .retrieve()
                    .toEntity(String.class);
            return ProcessorResponse.from(response);
        } catch (HttpServerErrorException e) {
            throw new TransientDownstreamException(
                    "processor returned " + e.getStatusCode(), e);
        } catch (ResourceAccessException e) {
            throw new TransientDownstreamException("processor unreachable", e);
        } catch (HttpClientErrorException e) {
            throw new PermanentDownstreamException(
                    "processor rejected the request with " + e.getStatusCode(), e);
        }
    }
}
