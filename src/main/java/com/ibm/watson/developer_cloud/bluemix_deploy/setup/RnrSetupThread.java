package com.ibm.watson.developer_cloud.bluemix_deploy.setup;

import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.lang.Exception;
import java.net.URL;
import java.net.URI;
import java.util.Collection;
import java.util.ArrayList;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import com.ibm.watson.developer_cloud.bluemix_deploy.listener.RnRConstants;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.RetrieveAndRank;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrClusterOptions;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster.Status;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrClusterList;

public class RnrSetupThread extends Thread {
	private static final Logger logger = LogManager.getLogger(RnrSetupThread.class.getName());
	public void run () {
				// TODO refactor into helper class or at least into methods
				System.out.println("1. Create RnR Service.");
				RetrieveAndRank service = new RetrieveAndRank();
				
				if (isAlreadySetup(service)) {
					return;
				}
				
				System.out.println("2. Create Cluster.");
				SolrCluster cluster = createCluster(service);
				System.out.println("3. Upload Cluster Configuration.");
				
				uploadConfiguration(service, cluster);
				System.out.println("4. Create Collection.");
				JsonObject vcap = new JsonParser().parse(System.getenv("VCAP_SERVICES")).getAsJsonObject();
                                JsonObject rr = vcap.getAsJsonArray("retrieve_and_rank").get(0).getAsJsonObject();
                                JsonObject credentials = rr.getAsJsonObject("credentials");
             
                                String username = credentials.get("username").getAsString();
                                String password = credentials.get("password").getAsString();
                                HttpSolrClient solrClient = getSolrClient(service.getSolrUrl(cluster.getId()), username, password);
				try{
				createCollection1(solrClient);
				System.out.println("5. Index Documents to Collection.");
				indexDocuments(solrClient);
					
				}catch(Exception e){
				System.out.println("Error initializing Collection"+e.getMessage());	
				}
	}

	/**
	 * Makes a call to get the number of clusters, if it is > 0
	 * then we assume the setup has already been done and we skip it
	 * 
	 * @param service
	 * @return
	 */
	private boolean isAlreadySetup(RetrieveAndRank service) {
		SolrClusterList clusters = service.getSolrClusters();

		return clusters.getSolrClusters().size() > 0 ? true : false;
	}

private static HttpSolrClient getSolrClient(String uri, String username, String password) {
    return new HttpSolrClient(uri, createHttpClient(uri, username, password));
  }

  private static HttpClient createHttpClient(String uri, String username, String password) {
    final URI scopeUri = URI.create(uri);

    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(new AuthScope(scopeUri.getHost(), scopeUri.getPort()),
        new UsernamePasswordCredentials(username, password));

    final HttpClientBuilder builder = HttpClientBuilder.create()
        .setMaxConnTotal(128)
        .setMaxConnPerRoute(32)
        .setDefaultRequestConfig(RequestConfig.copy(RequestConfig.DEFAULT).setRedirectsEnabled(true).build())
        .setDefaultCredentialsProvider(credentialsProvider)
        .addInterceptorFirst(new PreemptiveAuthInterceptor());
    return builder.build();
  }
  
   private static class PreemptiveAuthInterceptor implements HttpRequestInterceptor {
    public void process(final HttpRequest request, final HttpContext context) throws HttpException {
      final AuthState authState = (AuthState) context.getAttribute(HttpClientContext.TARGET_AUTH_STATE);

      if (authState.getAuthScheme() == null) {
        final CredentialsProvider credsProvider = (CredentialsProvider) context
            .getAttribute(HttpClientContext.CREDS_PROVIDER);
        final HttpHost targetHost = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
        final Credentials creds = credsProvider.getCredentials(new AuthScope(targetHost.getHostName(),
            targetHost.getPort()));
        if (creds == null) {
		  System.out.println("No creds provided for preemptive auth.");
          throw new HttpException("No creds provided for preemptive auth.");
        }
        authState.update(new BasicScheme(), creds);
      }
    }
  }
  
	private void indexDocuments(HttpSolrClient solrClient) {
		URL url = this.getClass().getClassLoader().getResource("file4.json");
		File dataFile = null;
		try {
                    dataFile = new File(url.toURI());
                } catch(Exception e) {
                    System.out.println(e.getMessage());;
               }
            
 JsonArray a =null;
 try{
 	a = (JsonArray)new JsonParser().parse(new FileReader(dataFile)).getAsJsonArray();
 }
 catch(Exception e){
 	System.out.println("Error parsing JSON document during indexing:"+e.getMessage());
 }
 Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
 for (int i = 0, size = a.size(); i < size; i++)
    {
  	SolrInputDocument document = new SolrInputDocument();
    JsonObject car = a.get(i).getAsJsonObject();

    int id = car.get(RnRConstants.SCHEMA_FIELD_ID).getAsInt();
    String title = (String) car.get(RnRConstants.SCHEMA_FIELD_TITLE).getAsString();
    String body = (String) car.get(RnRConstants.SCHEMA_FIELD_BODY).getAsString();
    String sourceUrl = (String) car.get(RnRConstants.SCHEMA_FIELD_SOURCE_URL).getAsString();
    String contentHtml = (String) car.get(RnRConstants.SCHEMA_FIELD_CONTENT_TEXT).getAsString();
    document.addField(RnRConstants.SCHEMA_FIELD_ID, id);
    document.addField(RnRConstants.SCHEMA_FIELD_TITLE, title);
    document.addField(RnRConstants.SCHEMA_FIELD_BODY, body);
    document.addField(RnRConstants.SCHEMA_FIELD_SOURCE_URL,sourceUrl);
    document.addField(RnRConstants.SCHEMA_FIELD_CONTENT_HTML, contentHtml);
    docs.add(document);
  }
		System.out.println("Indexing document...");

		UpdateResponse addResponse;
		try {
			addResponse = solrClient.add("car_collection", docs);
			
			System.out.println(addResponse);

			// Commit the document to the index so that it will be available for searching.
			solrClient.commit("car_collection");
			System.out.println("Indexed and committed document.");
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			System.out.println("Solr Exception while indexing:"+e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Solr IO Exception while indexing:"+e.getMessage());
		}
	}

private static void createCollection1(HttpSolrClient solrClient) {
    final CollectionAdminRequest.Create createCollectionRequest =
        new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName("car_collection");
    createCollectionRequest.setConfigName("car_config");

    System.out.println("Creating collection...");
	CollectionAdminResponse response = null;
    try {
		response = createCollectionRequest.process(solrClient);
	} catch (SolrServerException e) {
		// TODO Auto-generated catch block
		System.out.println(e.getMessage());
	} catch (IOException e) {
		// TODO Auto-generated catch block
		System.out.println(e.getMessage());
	}
    if (!response.isSuccess()) {
      System.out.println("Failed to create collection: "+response.getErrorMessages().toString());
    }
    System.out.println("Collection created.");
  }


	private void uploadConfiguration(RetrieveAndRank service, SolrCluster cluster) {
		URL url = this.getClass().getClassLoader().getResource("solr_config.zip");
		File configZip = null;
		try {
                    configZip = new File(url.toURI());
                } catch(Exception e) {
                    System.out.println("Error uploading configuration: "+e.getMessage());
               }
		
		
		// TODO extract name? error handling, check for 200
		service.uploadSolrClusterConfigurationZip(cluster.getId(),
		"car_config", configZip);
			System.out.println("Uploaded configuration.");
	}

	private SolrCluster createCluster(RetrieveAndRank service) {
		// 1 create the Solr Cluster
		// TODO place in easier to manipulate place? how large a cluster?
		SolrClusterOptions options = new SolrClusterOptions("car_cluster", 1);
		SolrCluster cluster = service.createSolrCluster(options);
		System.out.println("Solr cluster: " + cluster);
		
		// 2 wait until the Solr Cluster is available
		while (cluster.getStatus() == Status.NOT_AVAILABLE) {
		  try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		} // sleep 10 seconds
		  cluster = service.getSolrCluster(cluster.getId());
		  System.out.println("Solr cluster status: " + cluster.getStatus());
		}
		
		// 3 list Solr Clusters
		System.out.println("Solr clusters: " + service.getSolrClusters());
		
		return cluster;
	}
}
