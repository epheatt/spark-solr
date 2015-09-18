package com.lucidworks.spark;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.lucidworks.spark.query.PagedResultsIterator;
import com.lucidworks.spark.query.SolrTermVector;
import com.lucidworks.spark.query.StreamingResultsIterator;
import com.lucidworks.spark.util.SolrJsonSupport;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;

public class SolrRDD implements Serializable {

  public static Logger log = Logger.getLogger(SolrRDD.class);

  public static final int DEFAULT_PAGE_SIZE = 1000;

  public static SolrQuery ALL_DOCS = toQuery(null);

  public static SolrQuery toQuery(String queryString) {

    if (queryString == null || queryString.length() == 0)
      queryString = "*:*";

    SolrQuery q = new SolrQuery();
    if (queryString.indexOf("=") == -1) {
      // no name-value pairs ... just assume this single clause is the q part
      q.setQuery(queryString);
    } else {
      NamedList<Object> params = new NamedList<Object>();
      for (NameValuePair nvp : URLEncodedUtils.parse(queryString, StandardCharsets.UTF_8)) {
        String value = nvp.getValue();
        if (value != null && value.length() > 0) {
          String name = nvp.getName();
          if ("sort".equals(name)) {
            if (value.indexOf(" ") == -1) {
              q.addSort(SolrQuery.SortClause.asc(value));
            } else {
              String[] split = value.split(" ");
              q.addSort(SolrQuery.SortClause.create(split[0], split[1]));
            }
          } else {
            params.add(name, value);
          }
        }
      }
      q.add(ModifiableSolrParams.toSolrParams(params));
    }
    
    Integer rows = q.getRows();
    if (rows == null)
        q.setRows(DEFAULT_PAGE_SIZE);
    
    List<SolrQuery.SortClause> sorts = q.getSorts();
    if (sorts == null || sorts.isEmpty())
        q.addSort(SolrQuery.SortClause.asc(uniqueKey));
    
    return q;
  }

  /**
   * Iterates over the entire results set of a query (all hits).
   */
  private class QueryResultsIterator extends PagedResultsIterator<SolrDocument> {

    private QueryResultsIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark) {
      super(solrServer, solrQuery, cursorMark);
    }

    protected List<SolrDocument> processQueryResponse(QueryResponse resp) {
      return resp.getResults();
    }
  }

  /**
   * Returns an iterator over TermVectors
   */
  private class TermVectorIterator extends PagedResultsIterator<Vector> {

    private String field = null;
    private HashingTF hashingTF = null;

    private TermVectorIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark, String field, int numFeatures) {
      super(solrServer, solrQuery, cursorMark);
      this.field = field;
      hashingTF = new HashingTF(numFeatures);
    }

    protected List<Vector> processQueryResponse(QueryResponse resp) {
      NamedList<Object> response = resp.getResponse();

      NamedList<Object> termVectorsNL = (NamedList<Object>)response.get("termVectors");
      if (termVectorsNL == null)
        throw new RuntimeException("No termVectors in response! " +
          "Please check your query to make sure it is requesting term vector information from Solr correctly.");

      List<Vector> termVectors = new ArrayList<Vector>(termVectorsNL.size());
      Iterator<Map.Entry<String, Object>> iter = termVectorsNL.iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Object> next = iter.next();
        String nextKey = next.getKey();
        Object nextValue = next.getValue();
        if (nextValue instanceof NamedList) {
          NamedList nextList = (NamedList) nextValue;
          Object fieldTerms = nextList.get(field);
          if (fieldTerms != null && fieldTerms instanceof NamedList) {
            termVectors.add(SolrTermVector.newInstance(nextKey, hashingTF, (NamedList<Object>) fieldTerms));
          }
        }
      }

      SolrDocumentList docs = resp.getResults();
      totalDocs = docs.getNumFound();

      return termVectors;
    }
  }

  public static CloudSolrClient getSolrClient(String zkHost) {
    return SolrSupport.getSolrServer(zkHost);
  }

  protected String zkHost;
  protected String collection;
  protected static String uniqueKey = "id";

  public SolrRDD(String collection) {
    this("localhost:9983", collection); // assume local embedded ZK if not supplied
  }

  public SolrRDD(String zkHost, String collection) {
    this.zkHost = zkHost;
    this.collection = collection;
    try {
        String solrBaseUrl = getSolrBaseUrl(zkHost);
        // Hit Solr Schema API to get base information
        String schemaUrl = solrBaseUrl+collection+"/schema";
        try {
            Map<String, Object> schemaMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), schemaUrl, 2);
            this.uniqueKey = SolrJsonSupport.asString("/schema/uniqueKey", schemaMeta);
        } catch (SolrException solrExc) {
            log.warn("Can't get uniqueKey for " + collection+" due to: "+solrExc);
        }
    } catch (Exception exc) {
        log.warn("Can't get uniqueKey for " + collection+" due to: "+exc);
    }
  }

  private static String getSolrBaseUrl(String zkHost) throws Exception {
      CloudSolrClient solrServer = getSolrClient(zkHost);
      Set<String> liveNodes = solrServer.getZkStateReader().getClusterState().getLiveNodes();
      if (liveNodes.isEmpty())
        throw new RuntimeException("No live nodes found for cluster: "+zkHost);
      String solrBaseUrl = solrServer.getZkStateReader().getBaseUrlForNodeName(liveNodes.iterator().next());
      if (!solrBaseUrl.endsWith("?"))
        solrBaseUrl += "/";
      return solrBaseUrl;
  }
  
  /**
   * Get a document by ID using real-time get
   */
  public JavaRDD<SolrDocument> get(JavaSparkContext jsc, final String docId) throws SolrServerException {
    CloudSolrClient cloudSolrServer = getSolrClient(zkHost);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", collection);
    params.set("qt", "/get");
    params.set("id", docId);
    QueryResponse resp = null;
    try {
      resp = cloudSolrServer.query(params);
    } catch (Exception exc) {
      if (exc instanceof SolrServerException) {
        throw (SolrServerException)exc;
      } else {
        throw new SolrServerException(exc);
      }
    }
    SolrDocument doc = (SolrDocument) resp.getResponse().get("doc");
    List<SolrDocument> list = (doc != null) ? Arrays.asList(doc) : new ArrayList<SolrDocument>();
    return jsc.parallelize(list, 1);
  }

  public JavaRDD<SolrDocument> query(JavaSparkContext jsc, final SolrQuery query, boolean useDeepPagingCursor) throws SolrServerException {
    if (useDeepPagingCursor)
      return queryDeep(jsc, query);

    query.set("collection", collection);
    CloudSolrClient cloudSolrServer = getSolrClient(zkHost);
    List<SolrDocument> results = new ArrayList<SolrDocument>();
    Iterator<SolrDocument> resultsIter = new QueryResultsIterator(cloudSolrServer, query, null);
    while (resultsIter.hasNext()) results.add(resultsIter.next());
    return jsc.parallelize(results, 1);
  }

  /**
   * Makes it easy to query from the Spark shell.
   */
  public JavaRDD<SolrDocument> query(SparkContext sc, String queryStr) throws SolrServerException {
    return queryShards(new JavaSparkContext(sc), toQuery(queryStr));
  }

  public JavaRDD<SolrDocument> query(SparkContext sc, SolrQuery solrQuery) throws SolrServerException {
    return queryShards(new JavaSparkContext(sc), solrQuery);
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery) throws SolrServerException {
    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrClient(zkHost));

    final SolrQuery query = origQuery.getCopy();

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    List<SolrQuery.SortClause> sorts = query.getSorts();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));

    String fields = query.getFields();
    if (fields == null || fields.length() == 0) {
        query.setFields(getDefaultFields(query));
    }
    
    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String shardUrl) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*");
        }
      }
    );
    return docs;
  }

  public class ShardSplit implements Serializable {
    public SolrQuery query;
    public String shardUrl;
    public String rangeField;
    public long lowerInc;
    public Long upperExc;

    public ShardSplit(SolrQuery query, String shardUrl, String rangeField, long lowerInc, Long upperExc) {
      this.query = query;
      this.shardUrl = shardUrl;
      this.rangeField = rangeField;
      this.lowerInc = lowerInc;
      this.upperExc = upperExc;
    }

    public SolrQuery getSplitQuery() {
      SolrQuery splitQuery = query.getCopy();
      String upperClause = upperExc != null ? upperExc.toString()+"}" : "*]";
      splitQuery.addFilterQuery(rangeField+":["+lowerInc+" TO "+upperClause);
      return splitQuery;
    }
  }

  public JavaRDD<SolrDocument> queryShards(JavaSparkContext jsc, final SolrQuery origQuery, final String splitFieldName, final int splitsPerShard) throws SolrServerException {
    // if only doing 1 split per shard, then queryShards does that already
    if (splitFieldName == null || splitsPerShard <= 1)
      return queryShards(jsc, origQuery);

    long timerDiffMs = 0L;
    long timerStartMs = 0L;

    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrClient(zkHost));

    timerStartMs = System.currentTimeMillis();

    // we'll be directing queries to each shard, so we don't want distributed
    final SolrQuery query = origQuery.getCopy();
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    List<SolrQuery.SortClause> sorts = query.getSorts();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));
    
    String fields = query.getFields();
    if (fields == null || fields.length() == 0) {
        query.setFields(getDefaultFields(query));
    }
    
    JavaRDD<ShardSplit> splitsRDD = jsc.parallelize(shards, shards.size()).flatMap(new FlatMapFunction<String, ShardSplit>() {
      public Iterable<ShardSplit> call(String shardUrl) throws Exception {
        SolrQuery statsQuery = query.getCopy();
        statsQuery.clearSorts();
        statsQuery.setRows(0);
        statsQuery.set("stats", true);
        statsQuery.set("stats.field", splitFieldName);

        HttpSolrClient solrClient = SolrSupport.getHttpSolrClient(shardUrl);
        QueryResponse qr = solrClient.query(statsQuery);
        Map<String, FieldStatsInfo> statsInfoMap = qr.getFieldStatsInfo();
        FieldStatsInfo stats = (statsInfoMap != null) ? statsInfoMap.get(splitFieldName) : null;
        if (stats == null)
          throw new IllegalStateException("Failed to get stats for field '" + splitFieldName + "'!");

        Number min = (Number)stats.getMin();
        Number max = (Number)stats.getMax();

        long range = max.longValue() - min.longValue();
        if (range <= 0)
          throw new IllegalStateException("Field '" + splitFieldName + "' cannot be split into " + splitsPerShard + " splits; min=" + min + ", max=" + max);

        long bucketSize = Math.round(range / splitsPerShard);
        List<ShardSplit> splits = new ArrayList<ShardSplit>();
        long lowerInc = min.longValue();
        for (int b = 0; b < splitsPerShard; b++) {
          long upperExc = lowerInc + bucketSize;
          Long upperLimit = b < (splitsPerShard-1) ? upperExc : null;
          splits.add(new ShardSplit(query, shardUrl, splitFieldName, lowerInc, upperLimit));
          lowerInc = upperExc;
        }

        return splits;
      }
    });

    List<ShardSplit> splits = splitsRDD.collect();
    timerDiffMs = (System.currentTimeMillis() - timerStartMs);
    log.info("Collected "+splits.size()+" splits, took "+timerDiffMs+"ms");

    // parallelize the requests to the shards
    JavaRDD<SolrDocument> docs = jsc.parallelize(splits, splits.size()).flatMap(
      new FlatMapFunction<ShardSplit, SolrDocument>() {
        public Iterable<SolrDocument> call(ShardSplit split) throws Exception {
          return new StreamingResultsIterator(SolrSupport.getHttpSolrClient(split.shardUrl), split.getSplitQuery(), "*");
        }
      }
    );
    return docs;
  }

  public JavaRDD<Vector> queryTermVectors(JavaSparkContext jsc, final SolrQuery query, final String field, final int numFeatures) throws SolrServerException {
    // first get a list of replicas to query for this collection
    List<String> shards = buildShardList(getSolrClient(zkHost));

    if (query.getRequestHandler() == null) {
      query.setRequestHandler("/tvrh");
    }
    query.set("shards.qt", query.getRequestHandler());

    query.set("tv.fl", field);
    query.set("fq", field + ":[* TO *]"); // terms field not null!
    query.set("tv.tf_idf", "true");

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    List<SolrQuery.SortClause> sorts = query.getSorts();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));
    
    // parallelize the requests to the shards
    JavaRDD<Vector> docs = jsc.parallelize(shards, shards.size()).flatMap(
      new FlatMapFunction<String, Vector>() {
        public Iterable<Vector> call(String shardUrl) throws Exception {
          return new TermVectorIterator(SolrSupport.getHttpSolrClient(shardUrl), query, "*", field, numFeatures);
        }
      }
    );
    return docs;
  }

  // TODO: need to build up a LBSolrServer here with all possible replicas

  protected List<String> buildShardList(CloudSolrClient cloudSolrServer) {
    ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();

    ClusterState clusterState = zkStateReader.getClusterState();

    String[] collections = null;
    if (clusterState.hasCollection(collection)) {
      collections = new String[]{collection};
    } else {
      // might be a collection alias?
      Aliases aliases = zkStateReader.getAliases();
      String aliasedCollections = aliases.getCollectionAlias(collection);
      if (aliasedCollections == null)
        throw new IllegalArgumentException("Collection " + collection + " not found!");
      collections = aliasedCollections.split(",");
    }

    Set<String> liveNodes = clusterState.getLiveNodes();
    Random random = new Random(5150);

    List<String> shards = new ArrayList<String>();
    for (String coll : collections) {
      for (Slice slice : clusterState.getSlices(coll)) {
        List<String> replicas = new ArrayList<String>();
        for (Replica r : slice.getReplicas()) {
          ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
          if (liveNodes.contains(replicaCoreProps.getNodeName()))
            replicas.add(replicaCoreProps.getCoreUrl());
        }
        int numReplicas = replicas.size();
        if (numReplicas == 0)
          throw new IllegalStateException("Shard " + slice.getName() + " does not have any active replicas!");

        String replicaUrl = (numReplicas == 1) ? replicas.get(0) : replicas.get(random.nextInt(replicas.size()));
        shards.add(replicaUrl);
      }
    }
    return shards;
  }

  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery origQuery) throws SolrServerException {
    return queryDeep(jsc, origQuery, 36);
  }

  private String[] getDefaultFields(SolrQuery solrQuery) {
      try {
          StructType schema = getQuerySchema(solrQuery);
          StructField[] schemaFields = schema.fields();
          List<String> fieldList = new ArrayList<String>();
          for (int sf = 0; sf < schemaFields.length; sf++) {
              StructField schemaField = schemaFields[sf];
              Metadata meta = schemaField.metadata();
              Boolean isMultiValued = meta.contains("multiValued") ? meta.getBoolean("multiValued") : false;
              Boolean isDocValues = meta.contains("docValues") ? meta.getBoolean("docValues") : false;
              Boolean isStored = meta.contains("stored") ? meta.getBoolean("stored") : false;
              if (!isMultiValued && isDocValues && !isStored) {
                  fieldList.add(schemaField.name() + ":field("+schemaField.name()+")");
              } else if (!isMultiValued) {
                  fieldList.add(schemaField.name());
              }
          }
          String[] fieldArr = new String[fieldList.size()];
          fieldArr = fieldList.toArray(fieldArr);
          return fieldArr;
      } catch (Exception ex) {
          
      }
      String [] fieldAll = new String[1];
      fieldAll[0] = "*";
      return fieldAll;
  }
  
  public JavaRDD<SolrDocument> queryDeep(JavaSparkContext jsc, final SolrQuery origQuery, final int maxPartitions) throws SolrServerException {

    final SolrClient solrClient = getSolrClient(zkHost);
    final SolrQuery query = origQuery.getCopy();
    query.set("collection", collection);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(DEFAULT_PAGE_SIZE); // default page size

    List<SolrQuery.SortClause> sorts = query.getSorts();
    if (sorts == null || sorts.isEmpty())
        query.addSort(SolrQuery.SortClause.asc(uniqueKey));
    
    String fields = query.getFields();
    if (fields == null || fields.length() == 0) {
        query.setFields(getDefaultFields(query));
    }
    
    long startMs = System.currentTimeMillis();
    List<String> cursors = collectCursors(solrClient, query, true);
    long tookMs = System.currentTimeMillis() - startMs;
    log.info("Took "+tookMs+"ms to collect "+cursors.size()+" cursor marks");
    int numPartitions = Math.min(maxPartitions,cursors.size());

    JavaRDD<String> cursorJavaRDD = jsc.parallelize(cursors, numPartitions);
    // now we need to execute all the cursors in parallel
    JavaRDD<SolrDocument> docs = cursorJavaRDD.flatMap(
      new FlatMapFunction<String, SolrDocument>() {
        public Iterable<SolrDocument> call(String cursorMark) throws Exception {
          return querySolr(getSolrClient(zkHost), query, 0, cursorMark).getResults();
        }
      }
    );
    return docs;
  }

  protected List<String> collectCursors(final SolrClient solrClient, final SolrQuery origQuery) throws SolrServerException {
    return collectCursors(solrClient, origQuery, false);
  }

  protected List<String> collectCursors(final SolrClient solrClient, final SolrQuery origQuery, final boolean distrib) throws SolrServerException {
    List<String> cursors = new ArrayList<String>();

    final SolrQuery query = origQuery.getCopy();
    // tricky - if distrib == false, then set the param, otherwise, leave it out (default is distrib=true)
    if (!distrib) {
      query.set("distrib", false);
    } else {
      query.remove("distrib");
    }
    query.setFields(uniqueKey);

    String nextCursorMark = "*";
    while (true) {
      cursors.add(nextCursorMark);
      query.set("cursorMark", nextCursorMark);

      QueryResponse resp = null;
      try {
        resp = solrClient.query(query);
      } catch (Exception exc) {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException)exc;
        } else {
          throw new SolrServerException(exc);
        }
      }

      nextCursorMark = resp.getNextCursorMark();
      if (nextCursorMark == null || resp.getResults().isEmpty())
        break;
    }

    return cursors;
  }

  private static final Map<String,DataType> solrDataTypes = new HashMap<String, DataType>();
  static {
    solrDataTypes.put("solr.StrField", DataTypes.StringType);
    solrDataTypes.put("solr.TextField", DataTypes.StringType);
    solrDataTypes.put("solr.BoolField", DataTypes.BooleanType);
    solrDataTypes.put("solr.TrieIntField", DataTypes.IntegerType);
    solrDataTypes.put("solr.TrieLongField", DataTypes.LongType);
    solrDataTypes.put("solr.TrieFloatField", DataTypes.FloatType);
    solrDataTypes.put("solr.TrieDoubleField", DataTypes.DoubleType);
    solrDataTypes.put("solr.TrieDateField", DataTypes.TimestampType);
  }

  public DataFrame asTempTable(SQLContext sqlContext, String queryString, String tempTable) throws Exception {
    SolrQuery solrQuery = toQuery(queryString);
    DataFrame rows = applySchema(sqlContext, solrQuery, query(sqlContext.sparkContext(), solrQuery));
    rows.registerTempTable(tempTable);
    return rows;
  }

  public DataFrame queryForRows(SQLContext sqlContext, String queryString) throws Exception {
    SolrQuery solrQuery = toQuery(queryString);
    return applySchema(sqlContext, solrQuery, query(sqlContext.sparkContext(), solrQuery));
  }

  public DataFrame applySchema(SQLContext sqlContext, SolrQuery query, JavaRDD<SolrDocument> docs) throws Exception {
    // now convert each SolrDocument to a Row object
    StructType schema = getQuerySchema(query);
    JavaRDD<Row> rows = toRows(schema, docs);
    return sqlContext.applySchema(rows, schema);
  }

  public JavaRDD<Row> toRows(StructType schema, JavaRDD<SolrDocument> docs) {
    final StructField[] fields = schema.fields();
    JavaRDD<Row> rows = docs.map(new Function<SolrDocument, Row>() {
      public Row call(SolrDocument doc) throws Exception {
        Object[] vals = new Object[fields.length];
        for (int f = 0; f < fields.length; f++) {
          StructField field = fields[f];
          Object fieldValue = doc.getFieldValue(field.name());
          if (fieldValue != null) {
            if (fieldValue instanceof Collection) {
              vals[f] = ((Collection) fieldValue).toArray();
            } else {
              vals[f] = fieldValue;
            }
          }
        }
        return RowFactory.create(vals);
      }
    });
    return rows;
  }
  
  public StructType getQuerySchema(SolrQuery query) throws Exception {
    String solrBaseUrl = getSolrBaseUrl(zkHost);
    // Build up a schema based on the fields requested
    String fieldList = query.getFields();
    Map<String,SolrFieldMeta> fieldTypeMap = null;
    if (fieldList != null) {
      String[] fields = query.getFields().split(",");
      fieldTypeMap = getFieldTypes(fields, solrBaseUrl, collection);
    } else {
      fieldTypeMap = getSchemaFields(solrBaseUrl, collection);
    }

    if (fieldTypeMap == null || fieldTypeMap.isEmpty())
      throw new IllegalArgumentException("Query ("+query+") does not specify any fields needed to build a schema!");

    List<StructField> listOfFields = new ArrayList<StructField>();
    for (Map.Entry<String, SolrFieldMeta> field : fieldTypeMap.entrySet()) {
      SolrFieldMeta fieldMeta = field.getValue();
      MetadataBuilder metadata = new MetadataBuilder();
      DataType dataType = (fieldMeta != null && fieldMeta.fieldTypeClass != null) ? solrDataTypes.get(fieldMeta.fieldTypeClass) : null;
      if (dataType == null) dataType = DataTypes.StringType;

      if (fieldMeta.isMultiValued) {
        dataType = new ArrayType(dataType, true);
      }
      if (fieldMeta.isMultiValued) metadata.putBoolean("multiValued", fieldMeta.isMultiValued);      
      if (fieldMeta.isRequired) metadata.putBoolean("required", fieldMeta.isRequired);
      if (fieldMeta.isDocValues) metadata.putBoolean("docValues", fieldMeta.isDocValues);
      if (fieldMeta.isStored) metadata.putBoolean("stored", fieldMeta.isStored);
      if (fieldMeta.fieldType != null) metadata.putString("type", fieldMeta.fieldType);
      if (fieldMeta.dynamicBase != null) metadata.putString("dynamicBase", fieldMeta.dynamicBase);
      if (fieldMeta.fieldTypeClass != null) metadata.putString("class", fieldMeta.fieldTypeClass);      
      listOfFields.add(DataTypes.createStructField(field.getKey(), dataType, !fieldMeta.isRequired, metadata.build()));
    }

    return DataTypes.createStructType(listOfFields);
  }

  private static class SolrFieldMeta {
    String fieldType;
    String dynamicBase;
    boolean isRequired;
    boolean isMultiValued;
    boolean isDocValues;
    boolean isStored;
    String fieldTypeClass;
  }

  private static Map<String, SolrFieldMeta> getSchemaFields(String solrBaseUrl, String collection) {
      String lukeUrl = solrBaseUrl+collection+"/admin/luke?numTerms=0";
      //collect mapping of Solr field to type
      Map<String,SolrFieldMeta> schemaFieldMap = new HashMap<String,SolrFieldMeta>();
      try {
          try {
              Map<String, Object> adminMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), lukeUrl, 2);
              Map<String, Object> fields = SolrJsonSupport.asMap("/fields", adminMeta);
              for (Map.Entry<String, Object> field : fields.entrySet()) {
                  SolrFieldMeta tvc = new SolrFieldMeta();
                  tvc.dynamicBase = SolrJsonSupport.asString("/fields/"+field.getKey()+"/dynamicBase", adminMeta);
                  tvc = getFieldType(field.getKey(), tvc, solrBaseUrl, collection);
                  if (tvc != null) {
                      schemaFieldMap.put(field.getKey(), tvc);
                  }
              }
          } catch (SolrException solrExc) {
              log.warn("Can't get field type for field " + collection+" due to: "+solrExc);
          }
      } catch (Exception exc) {
          log.warn("Can't get schema fields for " + collection + " due to: "+exc);
      }
      return schemaFieldMap;
  }
  
  private static SolrFieldMeta getFieldType(String fieldName, SolrFieldMeta meta, String solrBaseUrl, String collection) {
      // Hit Solr Schema API to get field type for field
      String fieldUrl = solrBaseUrl+collection+"/schema/fields/"+fieldName;
      SolrFieldMeta tvc = null;
      try {
          try {
              Map<String, Object> fieldMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldUrl, 2);
              tvc = new SolrFieldMeta();
              tvc.fieldType = SolrJsonSupport.asString("/field/type", fieldMeta);
              tvc.isRequired = SolrJsonSupport.asBool("/field/required", fieldMeta);
              tvc.isMultiValued = SolrJsonSupport.asBool("/field/multiValued", fieldMeta);
              tvc.isDocValues = SolrJsonSupport.asBool("/field/docValues", fieldMeta);
              tvc.isStored = SolrJsonSupport.asBool("/field/stored", fieldMeta);
          } catch (SolrException solrExc) {
              int errCode = solrExc.code();
              if (errCode == 404) {
                  int lio = fieldName.lastIndexOf('_');
                  if (lio != -1 || (meta != null && meta.dynamicBase != null)) {
                      // see if the field is a dynamic field
                      String dynField = (meta != null && meta.dynamicBase != null) ? meta.dynamicBase : "*"+fieldName.substring(lio);
                      if (tvc == null) {
                          String dynamicFieldsUrl = solrBaseUrl+collection+"/schema/dynamicfields/"+dynField;
                          try {
                              Map<String, Object> dynFieldMeta =
                                      SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), dynamicFieldsUrl, 2);
                              tvc = new SolrFieldMeta();
                              tvc.fieldType = SolrJsonSupport.asString("/dynamicField/type", dynFieldMeta);
                              tvc.dynamicBase = dynField;
                              tvc.isRequired = SolrJsonSupport.asBool("/field/required", dynFieldMeta);
                              tvc.isMultiValued = SolrJsonSupport.asBool("/dynamicField/multiValued", dynFieldMeta);
                              tvc.isDocValues = SolrJsonSupport.asBool("/dynamicField/docValues", dynFieldMeta);
                              tvc.isStored = SolrJsonSupport.asBool("/dynamicField/stored", dynFieldMeta);
                          } catch (Exception exc) {
                              // just ignore this and throw the outer exc
                              log.error("Failed to get dynamic field information for "+dynField+" due to: "+exc);
                              throw solrExc;
                          }
                      }
                  }
              }
          }

          if (tvc == null || tvc.fieldType == null) {
              log.warn("Can't figure out field type for field: " + fieldName);
              return null;
          }
          if (!(tvc.isStored || tvc.isDocValues)) {
              log.warn("Can't retrieve an index only field: " + fieldName);
              //return null;
          }
          String fieldTypeUrl = solrBaseUrl+collection+"/schema/fieldtypes/"+tvc.fieldType;
          Map<String, Object> fieldTypeMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldTypeUrl, 2);
          tvc.fieldTypeClass = SolrJsonSupport.asString("/fieldType/class", fieldTypeMeta);

      } catch (Exception exc) {
          log.warn("Can't get field type for field " + fieldName+" due to: "+exc);
      }
      return tvc;
  }
  
  private static Map<String,SolrFieldMeta> getFieldTypes(String[] fields, String solrBaseUrl, String collection) {
    // collect mapping of Solr field to type
    Map<String,SolrFieldMeta> fieldTypeMap = new HashMap<String,SolrFieldMeta>();
    for (String field : fields) {
      if (fieldTypeMap.containsKey(field))
        continue;
      SolrFieldMeta tvc = getFieldType(field, null, solrBaseUrl, collection);
      if (tvc != null) {
          fieldTypeMap.put(field, tvc);
      }
    }
    return fieldTypeMap;
  }

  public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark) throws SolrServerException {
    return querySolr(solrServer, solrQuery, startIndex, cursorMark, null);
  }

  public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark, StreamingResponseCallback callback) throws SolrServerException {
    QueryResponse resp = null;
    try {
      if (cursorMark != null) {
        solrQuery.setStart(0);
        solrQuery.set("cursorMark", cursorMark);
      } else {
        solrQuery.setStart(startIndex);
      }

      if (callback != null) {
        resp = solrServer.queryAndStreamResponse(solrQuery, callback);
      } else {
        resp = solrServer.query(solrQuery);
      }
    } catch (Exception exc) {

      log.error("Query ["+solrQuery+"] failed due to: "+exc);

      // re-try once in the event of a communications error with the server
      Throwable rootCause = SolrException.getRootCause(exc);
      boolean wasCommError =
        (rootCause instanceof ConnectException ||
          rootCause instanceof IOException ||
          rootCause instanceof SocketException);
      if (wasCommError) {
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          if (callback != null) {
            resp = solrServer.queryAndStreamResponse(solrQuery, callback);
          } else {
            resp = solrServer.query(solrQuery);
          }
        } catch (Exception excOnRetry) {
          if (excOnRetry instanceof SolrServerException) {
            throw (SolrServerException)excOnRetry;
          } else {
            throw new SolrServerException(excOnRetry);
          }
        }
      } else {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException)exc;
        } else {
          throw new SolrServerException(exc);
        }
      }
    }

    return resp;
  }
}
