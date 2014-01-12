package io.teknek.yarn;

import java.io.PrintWriter;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;

public class TeknekYarnRunner {

  public static void main(String[] args) throws InterruptedException {
    System.getProperties().setProperty("hadoop.home.dir", "/home/edward/Downloads/hadoop-2.2.0");
    WeaveRunnerService runnerService = new YarnWeaveRunnerService(new YarnConfiguration(),
            "localhost:2181");
    
    runnerService.startAndWait();
    WeaveController controller = runnerService.prepare(new TeknekWeaveRunnable())
            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out))).start();
    Thread.sleep(90000);
  }
}
