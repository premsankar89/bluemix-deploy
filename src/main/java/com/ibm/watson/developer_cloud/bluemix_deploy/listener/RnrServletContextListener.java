package com.ibm.watson.developer_cloud.bluemix_deploy.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class RnrServletContextListener implements ServletContextListener {
   
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
	}

	public void contextInitialized(ServletContextEvent arg0) {
		System.out.println("testing that this worked");
	}
}
