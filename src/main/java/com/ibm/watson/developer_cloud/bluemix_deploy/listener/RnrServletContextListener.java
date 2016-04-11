package com.ibm.watson.developer_cloud.bluemix_deploy.listener;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.RetrieveAndRank;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster.Status;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrClusterOptions;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class RnrServletContextListener implements ServletContextListener {
   
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
	}

	public void contextInitialized(ServletContextEvent arg0) {
		// TODO need way to not do this if done before
		
		// TODO refactor into helper class or at least into methods
		System.out.println("got to 1");
		RetrieveAndRank service = new RetrieveAndRank();
		System.out.println("got to 2");
		SolrCluster cluster = createCluster(service);
		System.out.println("got to 5");
		
		uploadConfiguration(service, cluster);
		
		HttpSolrClient solrClient = new HttpSolrClient(service.getSolrUrl(cluster.getId()));
		
		createCollection(service, cluster, solrClient);
		
		indexDocuments(solrClient);
	}

	private void indexDocuments(HttpSolrClient solrClient) {
		SolrInputDocument document = new SolrInputDocument();
		document.addField("body", "experimental investigation of the aerodynamics of a wing in a slipstream .   an experimental study of a wing in a propeller slipstream was made in order to determine the spanwise distribution of the lift increase due to slipstream at different angles of attack of the wing and at different free stream to slipstream velocity ratios .  the results were intended in part as an evaluation basis for different theoretical treatments of this problem .   the comparative span loading curves, together with supporting evidence, showed that a substantial part of the lift increment produced by the slipstream was due to a /destalling/ or boundary-layer-control effect .  the integrated remaining lift increment, after subtracting this destalling lift, was found to agree well with a potential flow theory .   an empirical evaluation of the destalling effects was made for the specific configuration of the experiment .");
		document.addField("title", "experimental investigation of the aerodynamics of a wing in a slipstream .");

		System.out.println("Indexing document...");
		UpdateResponse addResponse;
		try {
			addResponse = solrClient.add("example_collection", document);
			
			System.out.println(addResponse);

			// Commit the document to the index so that it will be available for searching.
			solrClient.commit("example_collection");
			System.out.println("Indexed and committed document.");
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void createCollection(RetrieveAndRank service, SolrCluster cluster, HttpSolrClient solrClient) {
		CollectionAdminRequest.Create createCollectionRequest =
		        new CollectionAdminRequest.Create();
		createCollectionRequest.setCollectionName("example_collection");
		createCollectionRequest.setConfigName("example_config");

		System.out.println("Creating collection...");
		
		CollectionAdminResponse response;
		try {
			response = createCollectionRequest.process(solrClient);
			
			  if (!response.isSuccess()) {
			      System.out.println(response.getErrorMessages());
			      throw new IllegalStateException("Failed to create collection: "
			          + response.getErrorMessages().toString());
			    }
			System.out.println("Collection created.");
			System.out.println(response);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void uploadConfiguration(RetrieveAndRank service, SolrCluster cluster) {
		File configZip = new File("/resources/cranfield_solr_config.zip");
		
		// TODO extract name? error handling, check for 200
		service.uploadSolrClusterConfigurationZip(cluster.getId(),
		"example_config", configZip);
	}

	private SolrCluster createCluster(RetrieveAndRank service) {
		// 1 create the Solr Cluster
		// TODO place in easier to manipulate place? how large a cluster?
		SolrClusterOptions options = new SolrClusterOptions("my-cluster-name", 1);
		System.out.println("got to 3");
		SolrCluster cluster = service.createSolrCluster(options);
		System.out.println("got to 4");
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
		
		return cluster;
	}
}
