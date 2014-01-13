package io.teknek.yarn;

import io.teknek.daemon.TeknekDaemon;


import java.util.Properties;

public class TeknekYarnStarter {
  TeknekDaemon td;

  public void start() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, "localhost:2181");
    td = new TeknekDaemon(props);
    td.init();
  }
  
  public static void main (String [] args){
    TeknekYarnStarter d = new TeknekYarnStarter();
    d.start();
    while (true){
      System.out.println("Technique started" );
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}