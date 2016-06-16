package com.ibm.watson.developer_cloud.bluemix_deploy.setup;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.watson.developer_cloud.bluemix_deploy.listener.RnRConstants;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.RetrieveAndRank;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.Ranker;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrCluster.Status;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrClusterOptions;
import com.ibm.watson.developer_cloud.retrieve_and_rank.v1.model.SolrClusters;
import com.ibm.watson.developer_cloud.util.CredentialUtils;

public class RnrSetupThread extends Thread {
  private static final Logger logger = LogManager.getLogger(RnrSetupThread.class.getName());
  public static String RANKER_ID=null;

  public void run() {
    String username = "";
    String password = "";

    logger.info("0. Get username and password");
    String[] creds = CredentialUtils.getUserNameAndPassword("retrieve_and_rank");
    if (creds == null || creds.length != 2) {
      throw new IllegalArgumentException("The Retieve and Rank Credentials have not been specified.");
    }
    username = creds[0];
    password = creds[1];

    logger.info("1. Create RnR Service.");
    RetrieveAndRank service = new RetrieveAndRank();
    service.setUsernameAndPassword(username, password);

    if (isAlreadySetup(service)) {
      logger.info("A cluster is already setup,checking for a Ranker");
      RANKER_ID = System.getenv("RANKER_ID");
      if(RANKER_ID == null){
        if(service.getRankers().execute().getRankers().size() > 0){
          RANKER_ID = service.getRankers().execute().getRankers().get(0).getId();
        }
        if(RANKER_ID == null || RANKER_ID.isEmpty()){
          logger.info("Found a cluster setup but not a Ranker");
          logger.info("6. Create the Ranker");
          createRanker(service);
          return;
        }
      }
      logger.info("Found a cluster and ranker already setup.");
      return;
    }

    logger.info("2. Create Cluster.");
    SolrCluster cluster = createCluster(service);
    logger.info("3. Upload Cluster Configuration.");

    uploadConfiguration(service, cluster);
    logger.info("4. Create Collection.");
    HttpSolrClient solrClient = getSolrClient(service.getSolrUrl(cluster.getId()), username, password);
    try {
      createCollection(solrClient);
      logger.info("5. Index Documents to Collection.");
      indexDocuments(solrClient);
      
      logger.info("6. Create the Ranker");
      createRanker(service);

      logger.info("Setup complete");
      
    } catch (Exception e) {
      logger.error("Error initializing Collection" + e.getMessage());
    }
  }

  /**
   * Makes a call to get the number of clusters, if it is > 0 then we assume the setup has already
   * been done and we skip it
   * 
   * @param service
   * @return
   */
  private boolean isAlreadySetup(RetrieveAndRank service) {
    SolrClusters clusters = service.getSolrClusters().execute();
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

    final HttpClientBuilder builder = HttpClientBuilder.create().setMaxConnTotal(128).setMaxConnPerRoute(32)
        .setDefaultRequestConfig(RequestConfig.copy(RequestConfig.DEFAULT).setRedirectsEnabled(true).build())
        .setDefaultCredentialsProvider(credentialsProvider).addInterceptorFirst(new PreemptiveAuthInterceptor());
    return builder.build();
  }

  private static class PreemptiveAuthInterceptor implements HttpRequestInterceptor {
    public void process(final HttpRequest request, final HttpContext context) throws HttpException {
      final AuthState authState = (AuthState) context.getAttribute(HttpClientContext.TARGET_AUTH_STATE);

      if (authState.getAuthScheme() == null) {
        final CredentialsProvider credsProvider =
            (CredentialsProvider) context.getAttribute(HttpClientContext.CREDS_PROVIDER);
        final HttpHost targetHost = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
        final Credentials creds =
            credsProvider.getCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()));
        if (creds == null) {
          logger.error("No creds provided for preemptive auth.");
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
    } catch (Exception e) {
      logger.error(e.getMessage());;
    }

    JsonArray a = null;
    try {
      a = (JsonArray) new JsonParser().parse(new FileReader(dataFile)).getAsJsonArray();
    } catch (Exception e) {
      logger.error("Error parsing JSON document during indexing:" + e.getMessage());
    }
    Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0, size = a.size(); i < size; i++) {
      SolrInputDocument document = new SolrInputDocument();
      JsonObject car = a.get(i).getAsJsonObject();

      int id = car.get(RnRConstants.SCHEMA_FIELD_ID).getAsInt();
      String title = (String) car.get(RnRConstants.SCHEMA_FIELD_TITLE).getAsString();
      String body = (String) car.get(RnRConstants.SCHEMA_FIELD_BODY).getAsString();
      String sourceUrl = (String) car.get(RnRConstants.SCHEMA_FIELD_SOURCE_URL).getAsString();
      String contentHtml = (String) car.get(RnRConstants.SCHEMA_FIELD_CONTENT_HTML).getAsString();
      document.addField(RnRConstants.SCHEMA_FIELD_ID, id);
      document.addField(RnRConstants.SCHEMA_FIELD_TITLE, title);
      document.addField(RnRConstants.SCHEMA_FIELD_BODY, body);
      document.addField(RnRConstants.SCHEMA_FIELD_SOURCE_URL, sourceUrl);
      document.addField(RnRConstants.SCHEMA_FIELD_CONTENT_HTML, contentHtml);
      docs.add(document);
    }
    logger.info("Indexing document...");

    UpdateResponse addResponse;
    try {
      addResponse = solrClient.add(RnRConstants.COLLECTION_NAME, docs);

      logger.info(addResponse);

      // Commit the document to the index so that it will be available for searching.
      solrClient.commit(RnRConstants.COLLECTION_NAME);
      logger.info("Indexed and committed document.");
    } catch (SolrServerException e) {
      // TODO Auto-generated catch block
      logger.error("Solr Exception while indexing:" + e.getMessage());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      logger.error("Solr IO Exception while indexing:" + e.getMessage());
    }
  }

  private static void createCollection(HttpSolrClient solrClient) {
    final CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName(RnRConstants.COLLECTION_NAME);
    createCollectionRequest.setConfigName(RnRConstants.CONFIGURATION_NAME);

    logger.info("Creating collection...");
    CollectionAdminResponse response = null;
    try {
      response = createCollectionRequest.process(solrClient);
    } catch (SolrServerException e) {
      logger.error(e.getMessage());
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    if (!response.isSuccess()) {
      logger.error("Failed to create collection: " + response.getErrorMessages().toString());
    }
    logger.info("Collection created.");
  }


  private void uploadConfiguration(RetrieveAndRank service, SolrCluster cluster) {
    URL url = this.getClass().getClassLoader().getResource("solr_config.zip");
    File configZip = null;
    try {
      configZip = new File(url.toURI());
    } catch (Exception e) {
      logger.error("Error uploading configuration: " + e.getMessage());
    }
    // TODO extract name? error handling, check for 200
    service.uploadSolrClusterConfigurationZip(cluster.getId(), RnRConstants.CONFIGURATION_NAME, configZip).execute();
    logger.info("Uploaded configuration.");
  }

  private SolrCluster createCluster(RetrieveAndRank service) {
    // 1 create the Solr Cluster
    // TODO place in easier to manipulate place? how large a cluster?
    SolrClusterOptions options = new SolrClusterOptions(RnRConstants.CLUSTER_NAME, 1);
    SolrCluster cluster = (SolrCluster) service.createSolrCluster(options).execute();
    logger.info("Solr cluster: " + cluster);

    // 2 wait until the Solr Cluster is available
    while (cluster.getStatus() == Status.NOT_AVAILABLE) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      } // sleep 10 seconds
      cluster = (SolrCluster) service.getSolrCluster(cluster.getId()).execute();
      logger.info("Solr cluster status: " + cluster.getStatus());
    }
    
    // sleep even after cluster reports ready, they report it too soon sometimes
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    }

    // 3 list Solr Clusters
    logger.info("Solr clusters: " + service.getSolrClusters().execute());

    return cluster;
  }
  
  /**
   *  This method creates a ranker with the existing training data and 
   *  waits until the 'Training' phase is complete and the Ranker status is 'Available'.
   *  
   * @param service
   */
  private void createRanker(RetrieveAndRank service) {
    URL url = this.getClass().getClassLoader().getResource("trainingdata.csv");
    File trainingFile = null;
    try {
      trainingFile = new File(url.toURI());
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    Ranker ranker = service.createRanker(RnRConstants.RANKER_NAME, trainingFile).execute();
    String rankerId = ranker.getId();
    logger.info("Creating a ranker with the rankerId- "+  rankerId);
    ranker = service.getRankerStatus(rankerId).execute();
    logger.info(ranker.getStatus().toString());
    while (ranker.getStatus().toString().equalsIgnoreCase("Training")) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      } // sleep 10 seconds
      ranker = service.getRankerStatus(rankerId).execute();
      logger.info("Ranker status: " + ranker.getStatus());
    }
  }
  
}
