package ch.psi.daq.retrieval.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigurationRetrieval {

    public static class InvalidException extends RuntimeException {
        public InvalidException(String msg) {
            super(msg);
        }
    }

    public List<Node> nodes;
    public boolean mergeLocal;
    public List<Integer> splits;
    public ConfigurationDatabase database;
    public String backend;
    public String canonicalHostname;
    public String databufferBaseDir;
    public String databufferKeyspacePrefix;
    public boolean testPulseMap;

    @Override
    public String toString() {
        try {
            return new ObjectMapper(new JsonFactory()).writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            return String.format("%s", e);
        }
    }

    public void validate() {
        if (backend == null) {
            throw new InvalidException("backend missing");
        }
    }

}
