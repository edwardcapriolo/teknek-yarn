package io.teknek.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 *
 * $ bin/hadoop jar simple-yarn-app-1.0-SNAPSHOT.jar com.hortonworks.simpleyarnapp.Client /bin/date 2 /apps/simple/simple-yarn-app-1.0-SNAPSHOT.jar
 * @author edward
 *
 */
public class TeknekYarn {

  public static final String TEKNEK_APP_NAME = "teknek-yarn-app";
  
  /**
   * 
   * @throws IOException 
   * @throws YarnException
   * @return if tekenek running return application report else return null
   */
  public static ApplicationReport maybeFindReportForTeknek(YarnClient yarnClient, String appName) 
          throws YarnException, IOException{
    List<ApplicationReport> currentApplications = yarnClient.getApplications();
    for (ApplicationReport report : currentApplications){
      if (report.getName().equals(TEKNEK_APP_NAME)
              && (report.getYarnApplicationState() == YarnApplicationState.RUNNING || report
                      .getYarnApplicationState() == YarnApplicationState.SUBMITTED)) {
        return report;
      }
    }
    return null;
  }
  
  public static YarnClient createAndStartYarn(YarnConfiguration conf){
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    return yarnClient;
  }
  
  public static void main (String [] args) throws IllegalArgumentException, IOException, YarnException, InterruptedException{
    
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = createAndStartYarn(conf);
    ApplicationReport tekRunning = maybeFindReportForTeknek(yarnClient, TEKNEK_APP_NAME);
    if (tekRunning != null){
      System.out.println("Teknek already running or submitted.");
      System.out.println("Tracking url:" + tekRunning.getOriginalTrackingUrl());
      return;
    }
    
    int n = 2;
    if (args.length > 0) {
      n = Integer.parseInt(args[0]);
    }
    //String jarPath = TeknekApplicationMaster.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    String pth = "file:///tmp/teknek-yarn-0.0.1-SNAPSHOT-jar-with-dependencies.jar" ;
    Path jarPath = new Path(pth);
    jarPath = FileSystem.get(conf).makeQualified(jarPath);

    YarnClientApplication app = yarnClient.createApplication();
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    amContainer.setCommands(Collections.singletonList("$JAVA_HOME/bin/java" + " -Xmx256M"
            + " io.teknek.yarn.TeknekApplicationMaster "+n+" 1> " 
            + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2> "
            + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
            ));
    
    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    setupAppMasterJar(conf, jarPath, appMasterJar);
    amContainer.setLocalResources( Collections.singletonMap("teknek-yarn.jar", appMasterJar));
    
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(conf, appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);
    
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);
    
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setApplicationName(TEKNEK_APP_NAME); // application name
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue

    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    yarnClient.submitApplication(appContext);
    Thread.sleep(5000);
    printApplicationStatistics(yarnClient, appId);

  }
  
  private static void printApplicationStatistics(YarnClient yarnClient, ApplicationId appId) throws YarnException, IOException{
    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    appReport = yarnClient.getApplicationReport(appId);
    System.out.println("State:" + appState);
    System.out.println("Type:" + appReport.getApplicationType());
    System.out.println("Diagnostics:" + appReport.getDiagnostics());
    System.out.println("Url:" + appReport.getTrackingUrl());
  }
  
  private static void setupAppMasterJar(Configuration conf, Path jarPath, LocalResource appMasterJar) throws IOException {
    FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
  }
  
  /* This adds the yarn classpath to some other classpath */
  private static void setupAppMasterEnv(Configuration conf, Map<String, String> appMasterEnv) {
    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$()
            + File.separator + "*");
  }
}

/*
ApplicationReport appReport = yarnClient.getApplicationReport(appId);
YarnApplicationState appState = appReport.getYarnApplicationState();
while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED
        && appState != YarnApplicationState.FAILED) {
  try {
    Thread.sleep(5000);
  } catch (InterruptedException e) {
    e.printStackTrace();
  }
  appReport = yarnClient.getApplicationReport(appId);
  System.out.println(appReport.getApplicationType());
  System.out.println(appReport.getDiagnostics());
  System.out.println(appReport.getTrackingUrl());
  appState = appReport.getYarnApplicationState();
  
}
*/