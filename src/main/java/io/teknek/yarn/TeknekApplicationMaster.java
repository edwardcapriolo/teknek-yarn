package io.teknek.yarn;

import io.teknek.daemon.TeknekDaemon;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
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
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Make container requests to ResourceManager
    for (int i = 0; i < 1; ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      System.out.println("Making res-req " + i);
      rmClient.addContainerRequest(containerAsk);
    }

    // Obtain allocated containers and launch
    int allocatedContainers = 0;
    while (allocatedContainers < 1) {
      AllocateResponse response = rmClient.allocate(0);
      for (Container container : response.getAllocatedContainers()) {
        ++allocatedContainers;

        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx =
            Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(
            Collections.singletonList(
                    "$JAVA_HOME/bin/java" +
                            " -cp " + jarPath + 
                            " " + TeknekDaemon.class.getName() +
                //" 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                //" 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                           " 1> /tmp/am-out 2> /tmp/am-err" 
                ));
        System.out.println("Launching container " + allocatedContainers);
        nmClient.startContainer(container, ctx);
      }
      Thread.sleep(1000);
    }

    // Now wait for containers to complete
    int completedContainers = 0;
    while (completedContainers < 1) {
      AllocateResponse response = rmClient.allocate(completedContainers);
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        ++completedContainers;
        System.out.println("Completed container " + completedContainers);
      }
      Thread.sleep(100);
    }

    Thread.sleep(1000000);
    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(
        FinalApplicationStatus.SUCCEEDED, "", "");
  }
}
