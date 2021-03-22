package ch.psi.daq.retrieval.pod.api1;

import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.LoggerFactory;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
public class Aggregation {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Aggregation.class.getSimpleName());

    @JsonCreator
    Aggregation() {
        if (nrOfBins < 1) {
            nrOfBins = 20;
        }
        else if (nrOfBins > 1000) {
            nrOfBins = 1000;
        }
    }

    public Aggregation(List<String> operators, int nrOfBins) {
        this.operators = operators;
        this.nrOfBins = nrOfBins;
    }

    public List<String> operators;
    public int nrOfBins;

}
