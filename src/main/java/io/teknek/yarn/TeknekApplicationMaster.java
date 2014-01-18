package io.teknek.yarn;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class TeknekApplicationMaster {

  public static void main(String[] args) throws Exception {
    
    //String jarPath = TeknekDaemon.class.getProtectionDomain().getCodeSource().getLocation().getFile();

    //String jarPath = "/home/edward/Documents/java/teknek-yarn/target/teknek-yarn-0.0.1-SNAPSHOT-jar-with-dependencies.jar" ;
    String jarPath = "file:///tmp/teknek-yarn-0.0.1-SNAPSHOT-jar-with-dependencies.jar" ;
    int n = 1;
    if (args.length>0){
      n = Integer.parseInt(args[0]);
    }
    Configuration conf = new YarnConfiguration();

    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    System.out.println("registerApplicationMaster 0");
    rmClient.registerApplicationMaster("", 0, "");
    System.out.println("registerApplicationMaster 1");
    
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(2048);
    capability.setVirtualCores(1);

    /* for (int i=0; i< n;i++){
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      rmClient.addContainerRequest(containerAsk);
    } */
    
    int allocated = 0;
    while (allocated < n) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      rmClient.addContainerRequest(containerAsk);
      AllocateResponse response = rmClient.allocate(0);
      for (Container container : response.getAllocatedContainers()) {

        allocated++;
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList("$JAVA_HOME/bin/java" + " -cp " + jarPath + " "
                + TeknekYarnStarter.class.getName() + " 1>"
                + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
                + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        nmClient.startContainer(container, ctx);
        Thread.sleep(200);
        System.out.println("after startContainer ");
      }
    }
    System.out.println("Should be alll alocated");
    Thread.sleep(1000000);
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
  }
}
