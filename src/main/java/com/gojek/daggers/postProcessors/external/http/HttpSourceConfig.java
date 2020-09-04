package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpSourceConfig implements Serializable, SourceConfig {
    private String endpoint;
    private String verb;
    private String requestPattern;
    private String requestVariables;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    @SerializedName(value = "type",  alternate = { "Type", "TYPE" })
    private String type;
    private String capacity;
    @SerializedName(value = "headers",  alternate = { "Headers", "HEADERS" })
    private Map<String, String> headers;
    private Map<String, OutputMapping> outputMapping;
    @SerializedName(value = "metricId",  alternate = { "MetricId", "METRICID" })
    private String metricId;

    public HttpSourceConfig(String endpoint, String verb, String requestPattern, String requestVariables, String streamTimeout, String connectTimeout, boolean failOnErrors, String type, String capacity, Map<String, String> headers, Map<String, OutputMapping> outputMapping, String metricId) {
        this.endpoint = endpoint;
        this.verb = verb;
        this.requestPattern = requestPattern;
        this.requestVariables = requestVariables;
        this.streamTimeout = streamTimeout;
        this.connectTimeout = connectTimeout;
        this.failOnErrors = failOnErrors;
        this.type = type;
        this.capacity = capacity;
        this.headers = headers;
        this.outputMapping = outputMapping;
        this.metricId = metricId;
    }

    public Integer getConnectTimeout() {
        return Integer.parseInt(connectTimeout);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getVerb() {
        return verb;
    }

    public String getRequestVariables() {
        return requestVariables;
    }

    @Override
    public String getPattern() {
        return requestPattern;
    }

    @Override
    public String getVariables() {
        return requestVariables;
    }

    public Integer getStreamTimeout() {
        return Integer.valueOf(streamTimeout);
    }

    public boolean isFailOnErrors() {
        return failOnErrors;
    }

    @Override
    public String getMetricId() {
        return metricId;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, OutputMapping> getOutputMapping() {
        return outputMapping;
    }

    @Override
    public List<String> getOutputColumns() {
        return new ArrayList<>(outputMapping.keySet());
    }

    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("endpoint", endpoint);
        mandatoryFields.put("verb", verb);
        mandatoryFields.put("failOnErrors", failOnErrors);
        mandatoryFields.put("capacity", capacity);
        mandatoryFields.put("requestPattern", requestPattern);
        mandatoryFields.put("requestVariables", requestVariables);
        mandatoryFields.put("streamTimeout", streamTimeout);
        mandatoryFields.put("connectTimeout", connectTimeout);
        mandatoryFields.put("outputMapping", outputMapping);

        return mandatoryFields;
    }

    public Integer getCapacity() {
        return Integer.parseInt(capacity);
    }
}
