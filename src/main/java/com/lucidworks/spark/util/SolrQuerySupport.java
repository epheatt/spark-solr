package com.lucidworks.spark.util;

import com.lucidworks.spark.query.*;
import com.lucidworks.spark.rdd.SolrRDD;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class SolrQuerySupport implements Serializable {

  private static Logger log = Logger.getLogger(SolrQuerySupport.class);


  public static final Map<String,DataType> SOLR_DATA_TYPES = new HashMap<>();
  static {
    SOLR_DATA_TYPES.put("solr.StrField", DataTypes.StringType);
    SOLR_DATA_TYPES.put("solr.TextField", DataTypes.StringType);
    SOLR_DATA_TYPES.put("solr.BoolField", DataTypes.BooleanType);
    SOLR_DATA_TYPES.put("solr.TrieIntField", DataTypes.IntegerType);
    SOLR_DATA_TYPES.put("solr.TrieLongField", DataTypes.LongType);
    SOLR_DATA_TYPES.put("solr.TrieFloatField", DataTypes.FloatType);
    SOLR_DATA_TYPES.put("solr.TrieDoubleField", DataTypes.DoubleType);
    SOLR_DATA_TYPES.put("solr.TrieDateField", DataTypes.TimestampType);
  }

  public static class SolrFieldMeta {
    String fieldType;
    String dynamicBase;
    boolean isRequired;
    boolean isMultiValued;
    boolean isDocValues;
    boolean isStored;
    String fieldTypeClass;
  }

  public static String getUniqueKey(String zkHost, String collection) {
    try {
      String solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost);
      // Hit Solr Schema API to get base information
      String schemaUrl = solrBaseUrl + collection + "/schema";
      try {
        Map<String, Object> schemaMeta = SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), schemaUrl, 2);
        return SolrJsonSupport.asString("/schema/uniqueKey", schemaMeta);
      } catch (SolrException solrExc) {
        log.warn("Can't get uniqueKey for " + collection + " due to solr: " + solrExc);
      }
    } catch (Exception exc) {
      log.warn("Can't get uniqueKey for " + collection + " due to: " + exc);
    }
    return QueryConstants.DEFAULT_REQUIRED_FIELD;
  }

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
      q.setRows(QueryConstants.DEFAULT_PAGE_SIZE);

    return q;
  }

  public static SolrQuery addDefaultSort(SolrQuery q, String uniqueKey) {
    String sorts = q.getSortField();
    if (sorts == null || sorts.isEmpty())
      q.addSort(SolrQuery.SortClause.asc(uniqueKey));

    return q;
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

      Integer rows = solrQuery.getRows();
      if (rows == null)
        solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE);

      if (callback != null) {
        resp = solrServer.queryAndStreamResponse(solrQuery, callback);
      } else {
        resp = solrServer.query(solrQuery);
      }
    } catch (Exception exc) {

      log.error("Query [" + solrQuery + "] failed due to: " + exc);

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
            throw (SolrServerException) excOnRetry;
          } else {
            throw new SolrServerException(excOnRetry);
          }
        }
      } else {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException) exc;
        } else {
          throw new SolrServerException(exc);
        }
      }
    }

    return resp;
  }

  // Query defaults when directing queries to each Shard
  public static void setQueryDefaultsForShards(SolrQuery query, String uniqueKey) {
    query.set("distrib", "false");
    query.setStart(0);
    if (query.getRows() == null) {
      query.setRows(QueryConstants.DEFAULT_PAGE_SIZE);
    }
    SolrQuerySupport.addDefaultSort(query, uniqueKey);
  }

  // Query defaults for Term Vectors
  public static void setQueryDefaultsForTV(SolrQuery query, String field, String uniqueKey) {
     if (query.getRequestHandler() == null) {
      query.setRequestHandler("/tvrh");
    }
    query.set("shards.qt", query.getRequestHandler());

    query.set("tv.fl", field);
    query.set("fq", field + ":[* TO *]"); // terms field not null!
    query.set("tv.tf_idf", "true");

    // we'll be directing queries to each shard, so we don't want distributed
    query.set("distrib", false);
    query.setStart(0);
    if (query.getRows() == null)
      query.setRows(QueryConstants.DEFAULT_PAGE_SIZE); // default page size

    SolrQuerySupport.addDefaultSort(query, uniqueKey);
  }

  public static Map<String, SolrFieldMeta> getFieldTypes(String[] fields, String solrBaseUrl, String collection) {
    return getFieldTypes(fields, solrBaseUrl + collection + "/");
  }

  public static Map<String,SolrFieldMeta> getFieldTypes(String[] fields, String solrUrl) {

    // specific field list
    StringBuilder sb = new StringBuilder();
    if (fields.length > 0) sb.append("&fl=");
    for (int f=0; f < fields.length; f++) {
      if (f > 0) sb.append(",");
      sb.append(fields[f]);
    }
    String fl = sb.toString();

    String fieldsUrl = solrUrl + "schema/fields?showDefaults=true&includeDynamic=true"+fl;
    List<Map<String, Object>> fieldInfoFromSolr = null;
    try {
      Map<String, Object> allFields =
              SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldsUrl, 2);
      fieldInfoFromSolr = (List<Map<String, Object>>)allFields.get("fields");
    } catch (Exception exc) {
      String errMsg = "Can't get field metadata from Solr using request "+fieldsUrl+" due to: " + exc;
      log.error(errMsg);
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(errMsg, exc);
      }
    }

    // avoid looking up field types more than once
    Map<String,String> fieldTypeToClassMap = new HashMap<String,String>();

    // In case of empty, just return all fields with their respective types
    if (fields.length == 0) {
      List<String> fieldNames = new ArrayList<String>();
      for (Map<String,Object> map : fieldInfoFromSolr) {
         String fieldName = (String)map.get("name");
         fieldNames.add(fieldName);
      }
      fields = fieldNames.toArray(new String[fieldNames.size()]);
    }

    // collect mapping of Solr field to type
    Map<String,SolrFieldMeta> fieldTypeMap = new HashMap<String,SolrFieldMeta>();
    for (String field : fields) {

      if (fieldTypeMap.containsKey(field))
        continue;

      SolrFieldMeta tvc = null;
      for (Map<String,Object> map : fieldInfoFromSolr) {
        String fieldName = (String)map.get("name");
        if (field.equals(fieldName)) {
          tvc = new SolrFieldMeta();
          tvc.fieldType = (String)map.get("type");
          Object required = map.get("required");
          if (required != null && required instanceof Boolean) {
            tvc.isRequired = ((Boolean)required).booleanValue();
          } else {
            tvc.isRequired = "true".equals(String.valueOf(required));
          }
          Object multiValued = map.get("multiValued");
          if (multiValued != null && multiValued instanceof Boolean) {
            tvc.isMultiValued = ((Boolean)multiValued).booleanValue();
          } else {
            tvc.isMultiValued = "true".equals(String.valueOf(multiValued));
          }
          Object docValues = map.get("docValues");
          if (docValues != null && docValues instanceof Boolean) {
            tvc.isDocValues = ((Boolean)docValues).booleanValue();
          } else {
            tvc.isDocValues = "true".equals(String.valueOf(docValues));
          }
          Object stored = map.get("stored");
          if (stored != null && stored instanceof Boolean) {
            tvc.isStored = ((Boolean)stored).booleanValue();
          } else {
            tvc.isStored = "true".equals(String.valueOf(stored));
          }
          Object dynamicBase = map.get("dynamicBase");
          if (dynamicBase != null && dynamicBase instanceof String) {
            tvc.dynamicBase = (String)dynamicBase;
          }
        }
      }

      if (tvc == null || tvc.fieldType == null) {
        String errMsg = "Can't figure out field type for field: " + field + ". Check you Solr schema and retry.";
        log.error(errMsg);
        throw new RuntimeException(errMsg);
      }

      String fieldTypeClass = fieldTypeToClassMap.get(tvc.fieldType);
      if (fieldTypeClass != null) {
        tvc.fieldTypeClass = fieldTypeClass;
      } else {
        String fieldTypeUrl = solrUrl + "schema/fieldtypes/" + tvc.fieldType;
        try {
          Map<String, Object> fieldTypeMeta =
                  SolrJsonSupport.getJson(SolrJsonSupport.getHttpClient(), fieldTypeUrl, 2);
          tvc.fieldTypeClass = SolrJsonSupport.asString("/fieldType/class", fieldTypeMeta);
          fieldTypeToClassMap.put(tvc.fieldType, tvc.fieldTypeClass);
        } catch (Exception exc) {
          String errMsg = "Can't get field type metadata for "+tvc.fieldType+" from Solr due to: " + exc;
          log.error(errMsg);
          if (exc instanceof RuntimeException) {
            throw (RuntimeException)exc;
          } else {
            throw new RuntimeException(errMsg, exc);
          }
        }
      }
      if (!(tvc.isStored || tvc.isDocValues)) {
        log.info("Can't retrieve an index only field: " + field);
        tvc = null;
      }
      if (tvc != null && !tvc.isStored && tvc.isMultiValued && tvc.isDocValues) {
        log.info("Can't retrieve a non stored multiValued docValues field: " + field);
        tvc = null;
      }
      if (tvc != null) {
        fieldTypeMap.put(field, tvc);
      }
    }
    if (fieldTypeMap.isEmpty()) {
      log.warn("No readable fields found!");
    }
    return fieldTypeMap;
  }

  public static JavaRDD<ShardSplit> splitShard(JavaSparkContext jsc, final SolrQuery query, List<String> shards, final String splitFieldName, final int splitsPerShard, final String collection) {

    // get field type of split field
    final DataType fieldDataType;
    if ("_version_".equals(splitFieldName)) {
      fieldDataType = DataTypes.LongType;
    } else {
      Map<String, SolrFieldMeta> fieldMetaMap = SolrQuerySupport.getFieldTypes(new String[]{splitFieldName}, shards.get(0));
      SolrFieldMeta solrFieldMeta = fieldMetaMap.get(splitFieldName);
      if (solrFieldMeta != null) {
        String fieldTypeClass = solrFieldMeta.fieldTypeClass;
        fieldDataType = SolrQuerySupport.SOLR_DATA_TYPES.get(fieldTypeClass);
      } else {
        log.warn("No field metadata found for " + splitFieldName + ", assuming it is a String!");
        fieldDataType = DataTypes.StringType;
      }
      if (fieldDataType == null)
        throw new IllegalArgumentException("Cannot determine DataType for split field " + splitFieldName);
    }

    JavaRDD<ShardSplit> splitsRDD = jsc.parallelize(shards, shards.size()).flatMap(new FlatMapFunction<String, ShardSplit>() {
      public Iterable<ShardSplit> call(String shardUrl) throws Exception {

        ShardSplitStrategy splitStrategy = null;
        if (fieldDataType == DataTypes.LongType || fieldDataType == DataTypes.IntegerType) {
          splitStrategy = new NumberFieldShardSplitStrategy();
        } else if (fieldDataType == DataTypes.StringType) {
          splitStrategy = new StringFieldShardSplitStrategy();
        } else {
          throw new IllegalArgumentException("Can only split shards on fields of type: long, int, or string!");
        }
        List<ShardSplit> splits =
          splitStrategy.getSplits(shardUrl, query, splitFieldName, splitsPerShard);

        log.info("Found " + splits.size() + " splits for " + splitFieldName + ": " + splits);

        return splits;
      }
    });
    return splitsRDD;
  }

  public static final class PivotField implements Serializable {
    public final String solrField;
    public final String prefix;
    public final String otherSuffix;
    public final int maxCols;

    public PivotField(String solrField, String prefix) {
      this(solrField, prefix, 10);
    }

    public PivotField(String solrField, String prefix, int maxCols) {
      this(solrField, prefix, maxCols, "other");
    }

    public PivotField(String solrField, String prefix, int maxCols, String otherSuffix) {
      this.solrField = solrField;
      this.prefix = prefix;
      this.maxCols = maxCols;
      this.otherSuffix = otherSuffix;
    }
  }

  public static final int[] getPivotFieldRange(StructType schema, String pivotPrefix) {
    StructField[] schemaFields = schema.fields();
    int startAt = -1;
    int endAt = -1;
    for (int f=0; f < schemaFields.length; f++) {
      String name = schemaFields[f].name();
      if (startAt == -1 && name.startsWith(pivotPrefix)) {
        startAt = f;
      }
      if (startAt != -1 && !name.startsWith(pivotPrefix)) {
        endAt = f-1; // we saw the last field in the range before this field
        break;
      }
    }
    return new int[]{startAt,endAt};
  }

  public static final void fillPivotFieldValues(String rawValue, Object[] row, StructType schema, String pivotPrefix) {
    int[] range = getPivotFieldRange(schema, pivotPrefix);
    for (int i=range[0]; i <= range[1]; i++) row[i] = 0;
    try {
      row[schema.fieldIndex(pivotPrefix+rawValue.toLowerCase())] = 1;
    } catch (IllegalArgumentException ia) {
      row[range[1]] = 1;
    }
  }

  /**
   * Allows you to pivot a categorical field into multiple columns that can be aggregated into counts, e.g.
   * a field holding HTTP method (http_verb=GET) can be converted into: http_method_get=1, which is a common
   * task when creating aggregations.
   */
  public static DataFrame withPivotFields(final DataFrame solrData, final PivotField[] pivotFields, SolrRDD solrRDD, boolean escapeFieldNames) throws Exception {

    StructType schema = SolrSchemaUtil.getBaseSchema(solrRDD.getZKHost(), solrRDD.getCollection(), escapeFieldNames);
    final StructType schemaWithPivots = toPivotSchema(solrData.schema(), pivotFields, solrRDD.getCollection(), schema, solrRDD.getUniqueKey(), solrRDD.getZKHost());

    JavaRDD<Row> withPivotFields = solrData.javaRDD().map(new Function<Row, Row>() {
      @Override
      public Row call(Row row) throws Exception {
        Object[] fields = new Object[schemaWithPivots.size()];
        for (int c = 0; c < row.length(); c++)
          fields[c] = row.get(c);

        for (PivotField pf : pivotFields)
          SolrQuerySupport.fillPivotFieldValues(row.getString(row.fieldIndex(pf.solrField)), fields, schemaWithPivots, pf.prefix);

        return RowFactory.create(fields);
      }
    });

    return solrData.sqlContext().createDataFrame(withPivotFields, schemaWithPivots);
  }

  public static StructType toPivotSchema(final StructType baseSchema, final PivotField[] pivotFields, String collection, StructType schema, String uniqueKey, String zkHost) throws IOException, SolrServerException {
    List<StructField> pivotSchemaFields = new ArrayList<>();
    pivotSchemaFields.addAll(Arrays.asList(baseSchema.fields()));
    for (PivotField pf : pivotFields) {
      for (StructField sf : getPivotSchema(pf.solrField, pf.maxCols, pf.prefix, pf.otherSuffix, collection, schema, uniqueKey, zkHost)) {
        pivotSchemaFields.add(sf);
      }
    }
    return DataTypes.createStructType(pivotSchemaFields);
  }

  public static List<StructField> getPivotSchema(String fieldName, int maxCols, String fieldPrefix, String otherName, String collection, StructType schema, String uniqueKey, String zkHost) throws IOException, SolrServerException {
    final List<StructField> listOfFields = new ArrayList<StructField>();
    SolrQuery q = new SolrQuery("*:*");
    q.set("collection", collection);
    q.setFacet(true);
    q.addFacetField(fieldName);
    q.setFacetMinCount(1);
    q.setFacetLimit(maxCols);
    q.setRows(0);
    FacetField ff = SolrQuerySupport.querySolr(SolrSupport.getSolrServer(zkHost), q, 0, null).getFacetField(fieldName);
    for (FacetField.Count f : ff.getValues()) {
      listOfFields.add(DataTypes.createStructField(fieldPrefix+f.getName().toLowerCase(), DataTypes.IntegerType, false));
    }
    if (otherName != null) {
      listOfFields.add(DataTypes.createStructField(fieldPrefix+otherName, DataTypes.IntegerType, false));
    }
    return listOfFields;
  }

  /**
   * Iterates over the entire results set of a query (all hits).
   */
  public static class QueryResultsIterator extends PagedResultsIterator<SolrDocument> {

    public QueryResultsIterator(SolrClient solrServer, SolrQuery solrQuery, String cursorMark) {
      super(solrServer, solrQuery, cursorMark);
    }

    protected List<SolrDocument> processQueryResponse(QueryResponse resp) {
      return resp.getResults();
    }
  }
}