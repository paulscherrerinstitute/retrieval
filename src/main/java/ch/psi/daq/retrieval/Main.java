package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.utils.MainJProf;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
//import reactor.core.scheduler.Schedulers;
import reactor.tools.agent.ReactorDebugAgent;

@SpringBootApplication
public class Main implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
    }

    public static void main(final String[] args) {
        //Schedulers.enableMetrics();
        MainJProf.optionalLocateProfiler();
        ReactorDebugAgent.init();
        SpringApplication.run(Main.class, args);
    }

}
