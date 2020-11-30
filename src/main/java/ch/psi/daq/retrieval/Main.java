package ch.psi.daq.retrieval;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Main implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
    }

    public static void main(final String[] args) {
        JProfileController.findProfiler();
        SpringApplication.run(Main.class, args);
    }

}
