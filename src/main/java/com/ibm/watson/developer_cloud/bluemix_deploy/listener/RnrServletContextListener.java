package com.ibm.watson.developer_cloud.bluemix_deploy.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ibm.watson.developer_cloud.bluemix_deploy.setup.RnrSetupThread;

public class RnrServletContextListener implements ServletContextListener {
   
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
	}

	public void contextInitialized(ServletContextEvent arg0) {
		new RnrSetupThread().start();
	}
}
