package com.ibm.watson.developer_cloud.bluemix_deploy.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.RetrieveAndRank;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster.Status;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrClusterOptions;

public class RnrServletContextListener implements ServletContextListener {
   
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
	}

	public void contextInitialized(ServletContextEvent arg0) {
		// TODO refactor into helper class or at least into methods
		RetrieveAndRank service = new RetrieveAndRank();
		
		createCluster(service);
		
		
	}

	private void createCluster(RetrieveAndRank service) {
		// 1 create the Solr Cluster
		// TODO place in easier to manipulate place? how large a cluster?
		SolrClusterOptions options = new SolrClusterOptions("my-cluster-name", 1);
		SolrCluster cluster = service.createSolrCluster(options);
		System.out.println("Solr cluster: " + cluster);
		
		// 2 wait until the Solr Cluster is available
		while (cluster.getStatus() == Status.NOT_AVAILABLE) {
		  try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // sleep 10 seconds
		  cluster = service.getSolrCluster(cluster.getId());
		  System.out.println("Solr cluster status: " + cluster.getStatus());
		}
		
		// 3 list Solr Clusters
		System.out.println("Solr clusters: " + service.getSolrClusters());
	}
}
