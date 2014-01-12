package io.teknek.yarn;

import java.util.Properties;

import io.teknek.daemon.TeknekDaemon;

import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;


public class TeknekWeaveRunnable extends AbstractWeaveRunnable{

  private WeaveContext context;
  private TeknekDaemon td;
  
  @Override
  public void initialize(WeaveContext context) {
    super.initialize(context);
    this.context = context;
  }

  @Override
  public void stop() {
    td.stop();
  }

  @Override
  public void run() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, "localhost:2181");
    td = new TeknekDaemon(props);
    td.init();
    
  }

}
