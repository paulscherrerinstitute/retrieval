package ch.psi.daq.retrieval.utils;

import ch.psi.daq.retrieval.error.ErrorUtils;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class MainConf {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MainConf.class.getSimpleName());

    @Bean
    ErrorUtils cleaner() {
        LOGGER.info("make bean cleaner");
        return new ErrorUtils();
    }

}
