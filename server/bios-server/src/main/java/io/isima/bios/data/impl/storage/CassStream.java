/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.isima.bios.data.impl.storage;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.StreamConversion.AttributeConversion;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.TfosUtils;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.data.storage.cassandra.CassandraDataStoreUtils;
import io.isima.bios.errors.exception.FilterNotApplicableException;
import io.isima.bios.errors.exception.FilterSyntaxException;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Event;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.query.CompiledSortRequest;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import io.isima.bios.utils.StringUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that carries DB-related metadata for a data stream.
 *
 * <p>An instance of this class must be stateless and immutable once it is settled.
 */
// TODO(Naoki): Rename this to TableHandler
public abstract class CassStream {
  private static final Logger logger = LoggerFactory.getLogger(CassStream.class);

  private static final String FORMAT_DELETE_STREAM_TABLE = "DROP TABLE IF EXISTS %s.%s";

  /**
   * Creates a DataStream instance from a CassTenant and a StreamConfig objects.
   *
   * @param cassTenant CassTenant object to be used for building the instance.
   * @param streamDesc StreamConfig object to be used for building the instance.
   * @param dataEngine TODO
   * @return CassStream instance.
   */
  public static CassStream create(
      CassTenant cassTenant, StreamDesc streamDesc, DataEngineImpl dataEngine) {
    final CassStream cassStream;
    switch (streamDesc.getType()) {
      case SIGNAL:
        cassStream = new SignalCassStream(cassTenant, streamDesc);
        break;
      case CONTEXT:
        cassStream = new ContextCassStream(cassTenant, streamDesc);
        break;
      case METRICS:
        cassStream = new MetricsCassStream(cassTenant, streamDesc);
        break;
      case INDEX:
        cassStream = new IndexCassStream(cassTenant, streamDesc);
        break;
      case VIEW:
        cassStream = new ViewCassStream(cassTenant, streamDesc);
        break;
      case ROLLUP:
      case CONTEXT_FEATURE:
        cassStream = new RollupCassStream(cassTenant, streamDesc);
        break;
      case CONTEXT_INDEX:
        cassStream = new ContextIndexCassStream(cassTenant, streamDesc);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported StreamType: " + streamDesc.getType().name());
    }
    if (cassStream instanceof CassStream) {
      ((CassStream) cassStream).injectDataEngine(dataEngine);
    }
    return cassStream;
  }

  protected StreamDesc streamDesc;
  protected final CassTenant cassTenant;

  // TODO(Naoki): Better to be owned by CassandraConnection?
  protected final Map<String, PreparedStatement> allPreparedStatements = new HashMap<>();

  @Getter protected final String keyspaceName;
  protected final String tableName;
  String ingestQueryString;
  String deleteQueryString;
  PreparedStatement preparedIngest;
  protected final Map<String, CassAttributeDesc> attributeTable;

  protected final CassStream prev;

  protected CassandraConnection cassandraConnection;
  protected Session session;
  protected DataEngineImpl dataEngine;

  protected CassTableProperties propertiesImpl;

  /**
   * Default ttl set to the table.
   *
   * <p>Value -1 means unknown.
   */
  protected long defaultTtlInTable = -1;

  protected CassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    if (cassTenant == null) {
      throw new IllegalArgumentException("cassTenant may not be null");
    }
    if (!StreamConfig.validate(streamDesc)) {
      throw new IllegalArgumentException("invalid stream config: " + streamDesc);
    }
    this.streamDesc = streamDesc;
    this.cassTenant = cassTenant;

    keyspaceName = cassTenant.getKeyspaceName();
    final String tableNameSource;
    if (streamDesc.getSchemaName() != null) {
      tableName =
          generateTableName(
              streamDesc.getType(), streamDesc.getSchemaName(), streamDesc.getSchemaVersion());
    } else {
      tableName =
          generateTableName(
              streamDesc.getType(), streamDesc.getName(), streamDesc.getSchemaVersion());
    }
    attributeTable = new LinkedHashMap<>();
    StreamDesc prevDesc = streamDesc.getPrev();
    if (prevDesc != null) {
      CassStream prev = cassTenant.getCassStream(prevDesc, true);
      if (prev == null) {
        throw new IllegalStateException(
            "New StreamDesc has previous version but corresponding CassStream was not found");
      }
      this.prev = prev;
      prev.streamDesc = prevDesc;
      for (prevDesc = prevDesc.getPrev(); prevDesc != null; prevDesc = prevDesc.getPrev()) {
        prev = cassTenant.getCassStream(prevDesc, true);
        prev.streamDesc = prevDesc;
      }
    } else {
      this.prev = null;
    }

    if (streamDesc.getAttributes() != null) {
      for (AttributeDesc attributeDesc : streamDesc.getAttributes()) {
        addAttributeDesc(attributeDesc);
      }
    }
    if (streamDesc.getAdditionalAttributes() != null) {
      for (AttributeDesc attributeDesc : streamDesc.getAdditionalAttributes()) {
        addAttributeDesc(attributeDesc);
      }
    }

    ingestQueryString = generateIngestQueryString(cassTenant.getKeyspaceName());
    deleteQueryString = generateDeleteQueryString(cassTenant.getKeyspaceName());
  }

  protected void injectDataEngine(DataEngineImpl dataEngine) {
    this.dataEngine = dataEngine;
    if (dataEngine != null) {
      this.cassandraConnection = dataEngine.getCassandraConnection();
      if (this.cassandraConnection != null) {
        this.session = this.cassandraConnection.getSession();
        // this.session = cassandraConnection.getCluster().connect(keyspaceName);
      }
    }
  }

  public StreamDesc getStreamDesc() {
    return streamDesc;
  }

  public String getTenantName() {
    return cassTenant.getName();
  }

  public Long getTenantVersion() {
    return cassTenant.getVersion();
  }

  public String getStreamName() {
    return streamDesc.getName();
  }

  public Long getStreamVersion() {
    return streamDesc.getVersion();
  }

  public Long getSchemaVersion() {
    if (streamDesc.getSchemaVersion() != null) {
      return streamDesc.getSchemaVersion();
    } else {
      return streamDesc.getVersion();
    }
  }

  protected boolean hasTable() {
    return true;
  }

  public void setUpTable() throws ApplicationException {

    final String tenant = getTenantName();
    final String stream = streamDesc.getName();
    final String keyspace = cassTenant.getKeyspaceName();

    // Create the events table if missing
    Statement statement =
        new SimpleStatement(makeCreateTableStatement(keyspace))
            .setConsistencyLevel(ConsistencyLevel.ALL);
    logger.debug("Creating event table, statement={}", statement);

    RetryHandler retryHandler =
        new RetryHandler(
            "create table",
            logger,
            String.format(
                "tenant=%s, stream=%s, version=%d, keyspace=%s, table=%s",
                tenant, stream, streamDesc.getSchemaVersion(), keyspace, getTableName()));
    while (true) {
      try {
        cassandraConnection.verifyKeyspace(keyspace);
        if (cassandraConnection.verifyTable(keyspace, getTableName())) {
          return;
        }
        session.execute(statement);
        // Set the prepared statement for ingest
        logger.info(
            "create table done; tenant={}, stream={}, version={}({}), keyspace={}, table={}",
            tenant,
            stream,
            streamDesc.getSchemaVersion(),
            StringUtils.tsToIso8601(streamDesc.getSchemaVersion()),
            keyspace,
            getTableName());
        break;
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
  }

  /**
   * Method to initialize this stream database controller.
   *
   * <p>This method assumes that tables corresponding to this stream are created already. The
   * method, thus, should be called only at the final phase.
   *
   * @param keyspaceName Keyspace name
   * @throws ApplicationException When an unexpected error happens.
   */
  public void initialize(String keyspaceName) throws ApplicationException {
    if (preparedIngest == null && hasTable()) {
      final String key = keyspaceName + "." + getTableName();
      synchronized (allPreparedStatements) {
        try {
          PreparedStatement statement = allPreparedStatements.get(key);
          if (statement == null) {
            statement = cassandraConnection.getSession().prepare(getIngestQueryString());
            allPreparedStatements.put(key, statement);
          }
          preparedIngest = statement;
        } catch (DriverException e) {
          logger.warn(
              "Failed to make prepared statement; tenant={} stream={} version={}({})"
                  + ", exception={}",
              getTenantName(),
              streamDesc.getName(),
              streamDesc.getVersion(),
              StringUtils.tsToIso8601(streamDesc.getVersion()),
              e);
        }
      }
    }
  }

  public static String generateQualifiedNameSubstream(
      StreamType streamType, String streamName, String substreamName) {
    String qualifiedName;
    switch (streamType) {
      case VIEW:
        qualifiedName = AdminUtils.makeViewStreamName(streamName, substreamName);
        break;
      case INDEX:
        qualifiedName = AdminUtils.makeIndexStreamName(streamName, substreamName);
        break;
      case ROLLUP:
        qualifiedName = substreamName;
        break;
      case METRICS:
        qualifiedName = streamName;
        break;
      case CONTEXT_INDEX:
        qualifiedName = AdminUtils.makeContextIndexStreamName(streamName, substreamName);
        break;
      case CONTEXT_FEATURE:
        qualifiedName = AdminUtils.makeRollupStreamName(streamName, substreamName);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported streamType " + streamType);
    }
    return qualifiedName;
  }

  public CassStream getPrev() {
    return prev;
  }

  protected CassandraConnection getCassandraConnection() throws ApplicationException {
    if (cassandraConnection == null) {
      throw new ApplicationException("Cassandra connection is not set to this instance");
    }
    return cassandraConnection;
  }

  protected Session getSession() {
    return Objects.requireNonNull(session);
  }

  /**
   * Method to register a CassAttributeDesc instance using an AttributeDesc object.
   *
   * @param desc AttributeDesc object as the source of the registering instance.
   * @return Registered CassAttributeDesc object.
   */
  public CassAttributeDesc addAttributeDesc(AttributeDesc desc) {
    final CassAttributeDesc cassDesc =
        new CassAttributeDesc(desc, CassandraConstants.PREFIX_EVENTS_COLUMN);
    attributeTable.put(desc.getName().toLowerCase(), cassDesc);
    return cassDesc;
  }

  public CassAttributeDesc getAttributeDesc(String name) {
    return attributeTable.get(name.toLowerCase());
  }

  public Map<String, CassAttributeDesc> getAttributeTable() {
    return attributeTable;
  }

  public String getTableName() {
    return tableName;
  }

  public PreparedStatement getPreparedIngest() {
    return preparedIngest;
  }

  /**
   * Generate a statement to create a table for ingestion.
   *
   * @param keyspaceName Keyspace name.
   * @return Generated statement.
   */
  protected abstract String makeCreateTableStatement(String keyspaceName);

  /**
   * Generate a statement to delete tables when stream is deleted.
   *
   * @param keyspaceName Keyspace name.
   * @return Generated statement.
   */
  public String generateDeleteTableStatement(String keyspaceName) {
    return String.format(FORMAT_DELETE_STREAM_TABLE, keyspaceName, getTableName());
  }

  /**
   * Make a statement bound with values that is used for insertion.
   *
   * <p>The method takes input data as parameters, builds values to ingest, and generates a
   * statement by binding pre-defined ingest query string and the values.
   *
   * @param event Ingested event
   */
  public abstract Statement makeInsertStatement(final Event event) throws ApplicationException;

  /**
   * Generate a statement string for INGEST.
   *
   * <p>The generated statement is stored internally and is available by {@link
   * CassStream#getIngestQueryString()} method.
   *
   * @param keyspaceName Keyspace name
   * @return Generated prepared statement
   */
  protected abstract String generateIngestQueryString(String keyspaceName);

  /**
   * Make a statement bound with values that is used for deletion.
   *
   * <p>The method takes input data as parameters, builds values to delete, and generates a
   * statement by binding pre-defined delete query string and the values.
   *
   * @param event event to be deleted
   */
  public Statement makeDeleteStatement(final Event event) throws ApplicationException {
    throw new ApplicationException("Unimplemented method 'makeDeleteStatement'");
  }

  /**
   * Generate a statement string for delete.
   *
   * <p>The generated statement is stored internally and is available by {@link
   * CassStream#getDeleteQueryString()} method.
   *
   * @param keyspaceName Keyspace name
   * @return Generated prepared statement
   */
  protected String generateDeleteQueryString(String keyspaceName) {
    return null;
  }

  /**
   * Make query strings for an extraction.
   *
   * <p>The output may consists of multiple queries across partitions.
   *
   * @param state Operation execution state
   * @param attributes Collection of cass-attributes of the target stream.
   * @param startTime Start position of time range to specify. The value must be positive.
   * @param endTime End position of time range to specify. The end position may be less than
   *     startTime, but must be positive.
   * @return List of query info.
   * @throws ApplicationException when an unexpected error happens.
   * @throws TfosException When the request query is invalid
   * @throws IllegalArgumentException when a specified parameter is null. This exception is also
   *     thrown when specified time range has negative part.
   */
  public abstract List<QueryInfo> makeExtractStatements(
      QueryExecutionState state,
      Collection<CassAttributeDesc> attributes,
      Long startTime,
      Long endTime)
      throws ApplicationException, TfosException;

  /**
   * Returns ingest query string.
   *
   * @return Ingest query string
   */
  public String getIngestQueryString() {
    return ingestQueryString;
  }

  /**
   * Returns delete query string.
   *
   * @return Delete query string
   */
  public String getDeleteQueryString() {
    return deleteQueryString;
  }

  /**
   * Method to parse filter.
   *
   * @param srcFilter Source filter string
   * @return Parsed intermediate object (list of CQL Relations)
   * @throws FilterSyntaxException when the source string has a syntax error
   * @throws ApplicationException when an unexpected error happened
   */
  public static List<SingleColumnRelation> parseFilter(String srcFilter)
      throws FilterSyntaxException, InvalidFilterException, ApplicationException {
    try {
      final ByteArrayInputStream bais = new ByteArrayInputStream(srcFilter.getBytes());
      final ANTLRInputStream antlrInputStream = new ANTLRInputStream(bais);
      final CqlLexer lexer = new CqlLexer(antlrInputStream);
      final CommonTokenStream token = new CommonTokenStream(lexer);
      final CqlParser parser = new CqlParser(token);
      final var relations = new ArrayList<SingleColumnRelation>();
      for (var relation : parser.whereClause().build().relations) {
        if (!(relation instanceof SingleColumnRelation)) {
          throw new InvalidFilterException("Unsupported query where clause: " + srcFilter);
        }
        relations.add((SingleColumnRelation) relation);
      }
      if (lexer.getNumberOfSyntaxErrors() > 0
          || parser.getNumberOfSyntaxErrors() > 0
          || lexer.getCharPositionInLine() < srcFilter.length()) {
        throw new FilterSyntaxException();
      }
      return Collections.unmodifiableList(relations);
    } catch (IOException ioe) {
      throw new ApplicationException(
          String.format("Error occurred while processing filter  - '%s'", ioe.getMessage()), ioe);
    } catch (RecognitionException re) {
      throw new FilterSyntaxException(re.getMessage());
    }
  }

  /**
   * Method to verify an extraction filter and return the filter string interpreted to Cassandra
   * query.
   *
   * @param relations Source CQL Relations list. Actual type of relation must be
   *     org.apache.cassandra.cql3.Relation, but we use Objet here for hiding dependency on
   *     Cassandra to outside TFOS components.
   * @return Filter string to be applied to Cassandra. If the validator considers that the filter
   *     would never match, the method returns null.
   * @throws InvalidFilterException When the filter is semantically incorrect
   * @throws FilterNotApplicableException When the filter is not applicable to the target stream
   */
  public String interpretFilter(List<SingleColumnRelation> relations)
      throws InvalidFilterException, FilterNotApplicableException {
    return traverseFilter(relations, true);
  }

  /**
   * Method to validate an extraction filter.
   *
   * @param relations Source CQL Relations list. Actual type of relation must be
   *     org.apache.cassandra.cql3.Relation, but we use Objet here for hiding dependency on
   * @throws InvalidFilterException When the filter is semantically incorrect
   * @throws FilterNotApplicableException When the filter is not applicable to the target stream
   */
  public void validateFilter(List<SingleColumnRelation> relations)
      throws InvalidFilterException, FilterNotApplicableException {
    traverseFilter(relations, false);
  }

  /**
   * Method that traverses specified list of filter relations to verify and optionally interpret the
   * filter.
   *
   * @param relations The filter relations to traverse.
   * @param doInterpret Flag to determine whether the method runs interpretation.
   * @return When doInterpret == true, returns filter string interpreted to Cassandra WHERE clause,
   *     but returns null if the validator considers that there would be no match in this signal.
   *     When doInterpret is false, always returns null.
   * @throws InvalidFilterException When the filter is semantically incorrect
   * @throws FilterNotApplicableException When the filter is not applicable to the target stream
   */
  protected String traverseFilter(List<SingleColumnRelation> relations, boolean doInterpret)
      throws InvalidFilterException, FilterNotApplicableException {
    final StringBuilder sb = doInterpret ? new StringBuilder() : null;
    if (!validateRelations(relations, sb, null)) {
      // the filter is invalid for this stream
      return null;
    }
    if (doInterpret) {
      final String output = sb.toString();
      logger.debug("FILTER={}", output);
      return output;
    }
    return null;
  }

  /**
   * Validates a filter relations.
   *
   * <p>The method also interprets the relations to a filter string for internal table when the
   * StringBuilder parameter sb is given. In the interpretation, this stream/signal may be an old
   * version. In such a case, the method also converts the filter to fit the schema of the old
   * table.
   *
   * @param relations Relations to validate
   * @param sb StringBuilder to append interpreted filter string. The method skips interpretation
   *     and only runs validation when this parameter is null.
   * @param targetAttributes The method fills attributes to filter to this map when specified. If
   *     the parameter can be null.
   * @return In some conversion case, the specified filter is known to have no match. The method
   *     returns false in the case, so that the caller would skip making query statements. The
   *     method returns true otherwise.
   * @throws InvalidFilterException Thrown to indicate that the specified relations are invalid.
   * @throws FilterNotApplicableException Thrown to indicate that the specified relations are not
   *     applicable to the stream/signal. There are also cases of filter not applicable to old
   *     version of signal during filter conversion. But in such a case, the method returns false
   *     instead of throwing this exception to handle the case differently. (TODO: We should handle
   *     this better)
   */
  protected boolean validateRelations(
      List<SingleColumnRelation> relations, StringBuilder sb, Map<String, Boolean> targetAttributes)
      throws InvalidFilterException, FilterNotApplicableException {

    for (final var relation : relations) {
      boolean doConvertFilter = false;
      final String entity = relation.getEntity().rawText();
      CassAttributeDesc attributeDesc = getAttributeDesc(entity);
      final AttributeConversion conv =
          streamDesc.getStreamConversion() != null
              ? streamDesc.getStreamConversion().getAttributeConversion(entity)
              : null;
      if (conv != null) {
        switch (conv.getConversionType()) {
          case NO_CHANGE:
            // just continue
            break;
          case ADD:
            {
              if (testAttributeValue(relation, conv.getNewDesc(), conv.getDefaultValue())) {
                // always matches, continue without adding the filter
                continue;
              } else {
                // never matches, abort
                return false;
              }
            }
          case CONVERT:
            var oldAttributeType = conv.getOldDesc().getAttributeType();
            final Term.Raw value = relation.getValue();
            if (value != null) {
              try {
                oldAttributeType.parse(
                    getFilterRawValue(value.getText(), conv.getOldDesc(), conv.getNewDesc()));
              } catch (InvalidValueSyntaxException | InvalidFilterException e) {
                // Unable to build the filter for this version of signal.
                // We assume the filter validation is done before filter conversion happens.
                // So the filter is valid, but is not applicable to this version of the signal.
                // We treat this version of signal to have no match.
                return false;
              }
            } else {
              boolean mayMatch = false;
              for (var inValue : relation.getInValues()) {
                try {
                  oldAttributeType.parse(
                      getFilterRawValue(inValue.getText(), conv.getOldDesc(), conv.getNewDesc()));
                } catch (InvalidValueSyntaxException e) {
                  continue;
                }
                mayMatch = true;
                break;
              }
              if (!mayMatch) {
                return false;
              }
            }
            doConvertFilter = true;
            break;
          default:
            throw new UnsupportedOperationException("TODO: Implement this");
        }
      }
      if (attributeDesc == null) {
        throw new FilterNotApplicableException(
            String.format("Attribute '%s' is not in stream attributes", entity));
      }
      // TODO(Naoki): BLOB filter is not yet supported
      if (attributeDesc.getAttributeType() == InternalAttributeType.BLOB) {
        throw new InvalidFilterException("Filtering by BLOB is not supported");
      }
      if (attributeDesc.getAttributeType() == InternalAttributeType.ENUM) {
        switch (relation.operator()) {
          case GT:
          case LT:
          case GTE:
          case LTE:
            throw new InvalidFilterException("Cannot use <,> operator on ENUM types");
          default:
            break;
        }
      }
      if (sb != null && sb.length() > 0) {
        sb.append(" AND ");
      }
      if (targetAttributes != null) {
        final Operator operator = relation.operator();
        final Boolean isEquality = operator == Operator.EQ || operator == Operator.IN;
        targetAttributes.put(attributeDesc.getName(), isEquality);
      }
      if (sb != null) {
        sb.append(attributeDesc.getColumn()).append(" ").append(relation.operator());
      }

      final Term.Raw value = relation.getValue();
      if (value != null) {
        if (sb != null) {
          final String filterValue;
          if (doConvertFilter) {
            final var raw =
                getFilterRawValue(value.getText(), conv.getOldDesc(), conv.getNewDesc());
            filterValue =
                conv.getOldDesc().getAttributeType() == InternalAttributeType.STRING
                    ? "'" + raw + "'"
                    : raw;
          } else {
            filterValue = getFilterValue(value.getText(), attributeDesc);
          }
          sb.append(" ").append(filterValue);
        }
      } else {
        List<? extends Term.Raw> inValues = relation.getInValues();
        if (sb != null) {
          sb.append(" (");
        }
        String delimiter = "";
        for (Term.Raw elem : inValues) {
          final String filterValue;
          if (doConvertFilter) {
            final var oldType = conv.getOldDesc().getAttributeType();
            final var raw = getFilterRawValue(elem.getText(), conv.getOldDesc(), conv.getNewDesc());
            try {
              oldType.parse(raw);
            } catch (InvalidValueSyntaxException e) {
              // skip this value
              continue;
            }
            filterValue = oldType == InternalAttributeType.STRING ? "'" + raw + "'" : raw;
          } else {
            filterValue = getFilterValue(elem.getText(), attributeDesc);
          }
          if (sb != null) {
            sb.append(delimiter).append(filterValue);
          }
          delimiter = ", ";
        }
        if (sb != null) {
          sb.append(")");
        }
      }
    }
    return true;
  }

  protected Object getTypeConvertedFilterValue(Term.Raw value, AttributeDesc attrDesc)
      throws InvalidFilterException {
    final String valueAsString = getFilterRawValue(value.getText(), null, attrDesc);
    try {
      final var attributeType = attrDesc.getAttributeType();
      return attributeType == InternalAttributeType.ENUM
          ? Integer.valueOf(valueAsString)
          : attributeType.parse(valueAsString);
    } catch (InvalidValueSyntaxException e) {
      throw new InvalidFilterException(e.getMessage());
    }
  }

  /**
   * Test an attribute value using the specified relation.
   *
   * @param relation Relation to be used for testing the value
   * @param attrDesc Attribute description of the testing value
   * @param testingValue Testing attribute value
   * @return True if the value is tested positive, false otherwise
   * @throws InvalidFilterException Thrown to indicate that the value is unable to test
   */
  private boolean testAttributeValue(Relation relation, AttributeDesc attrDesc, Object testingValue)
      throws InvalidFilterException {
    final Term.Raw value = relation.getValue();
    if (value != null) {
      final Object typeConvertedValue = getTypeConvertedFilterValue(relation.getValue(), attrDesc);
      switch (relation.operator()) {
        case EQ:
          return typeConvertedValue.equals(testingValue);
        case GT:
          return ((Comparable) typeConvertedValue).compareTo((Comparable) testingValue) < 0;
        case GTE:
          return ((Comparable) typeConvertedValue).compareTo((Comparable) testingValue) <= 0;
        case LT:
          return ((Comparable) typeConvertedValue).compareTo((Comparable) testingValue) > 0;
        case LTE:
          return ((Comparable) typeConvertedValue).compareTo((Comparable) testingValue) >= 0;
        default:
          throw new InvalidFilterException(
              "Unsupported \"" + relation.operator() + "\" relation: " + relation);
      }
    } else {
      final String defaultValueAsString = testingValue.toString();
      for (var inValue : relation.getInValues()) {
        final String valueAsString = getFilterRawValue(inValue.getText(), null, attrDesc);
        if (valueAsString.equals(defaultValueAsString)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Converts filter rhs input to Cassandra internal format.
   *
   * <p>Types ENUM and BLOB requires special handling as follows. Otherwise the method returns the
   * input as is.
   *
   * <dl>
   *   <dt>ENUM
   *   <dd>Input ENUM name string should be converted to integer form which is ENUM's internal data
   *       type.
   *   <dt>BLOB
   *   <dd>TFOS representation of BLOB is base64, but Cassandra requires it to be hexadecimal
   *       string.
   * </dl>
   *
   * @param src Source input value string
   * @param attributeDesc The value's attribute description
   * @return Converted value string
   * @throws InvalidFilterException In case string conversion failed
   */
  protected static String getFilterValue(final String src, final AttributeDesc attributeDesc)
      throws InvalidFilterException {
    InternalAttributeType type = attributeDesc.getAttributeType();
    if (type != InternalAttributeType.ENUM && type != InternalAttributeType.BLOB) {
      return src;
    }
    if (!src.startsWith("'") || !src.endsWith("'")) {
      throw new InvalidFilterException(
          type.name().substring(0, 1)
              + type.name().substring(1).toLowerCase()
              + " should be represented as a string");
    }
    final String enumName = src.substring(1, src.length() - 1).replace("\\'", "'");
    Object value;
    try {
      value = Attributes.convertValue(enumName, attributeDesc);
    } catch (InvalidValueSyntaxException | InvalidEnumException e) {
      throw new InvalidFilterException(e.getMessage());
    }
    if (type == InternalAttributeType.ENUM) {
      return value.toString();
    } else { // blob
      ByteBuffer buf = (ByteBuffer) value;
      StringBuilder sb = new StringBuilder("0x");
      while (buf.hasRemaining()) {
        TfosUtils.byteToHexString(buf.get(), sb);
      }
      return sb.toString();
    }
  }

  protected static String getFilterRawValue(
      String src, AttributeDesc oldAttributeDesc, AttributeDesc newAttributeDesc)
      throws InvalidFilterException {

    final var enumOrBlob = Set.of(InternalAttributeType.ENUM, InternalAttributeType.BLOB);

    final InternalAttributeType newType = newAttributeDesc.getAttributeType();
    if (enumOrBlob.contains(newType)) {
      return getFilterValue(src, newAttributeDesc);
    }

    if (oldAttributeDesc != null) {
      final InternalAttributeType oldType = oldAttributeDesc.getAttributeType();
      if (enumOrBlob.contains(oldType)) {
        return getFilterValue(src, oldAttributeDesc);
      }
    }

    // If the new type is string, the filter value would come with string format
    if (newType == InternalAttributeType.STRING) {
      if (src.length() < 2 || src.charAt(0) != '\'' || src.charAt(src.length() - 1) != '\'') {
        throw new InvalidFilterException(
            String.format(
                "Value of attribute %s of type String must be a quoted string",
                newAttributeDesc.getName()));
      }
      return src.substring(1, src.length() - 1).replace("\\'", "'");
    } else {
      return src;
    }
  }

  /**
   * Utility method to convert a ValueType entry to Cassandra data type name.
   *
   * @param attributeType Type as a ValueType enum entry.
   * @return Corresponding Cassandra data type name.
   */
  protected static String valueTypeToCassDataTypeName(InternalAttributeType attributeType) {
    return valueTypeToCassDataType(attributeType).toString();
  }

  /**
   * Utility method to convert a ValueType entry to Cassandra data type.
   *
   * @param attributeType Type as a ValueType enum entry.
   * @return Corresponding Cassandra data type.
   */
  protected static DataType valueTypeToCassDataType(InternalAttributeType attributeType) {
    switch (attributeType) {
      case STRING:
        return DataType.text();
      case INT:
        return DataType.cint();
      case LONG:
        return DataType.bigint();
      case NUMBER:
        return DataType.varint();
      case DOUBLE:
        return DataType.cdouble();
      case INET:
        return DataType.inet();
      case DATE:
        return DataType.date();
      case TIMESTAMP:
        return DataType.timestamp();
      case UUID:
        return DataType.uuid();
      case BOOLEAN:
        return DataType.cboolean();
      case ENUM:
        return DataType.cint();
      case BLOB:
        return DataType.blob();
      default:
        throw new IllegalArgumentException("Unknown attribute type " + attributeType);
    }
  }

  /**
   * A utility method to populate event attribute values to an Object array.
   *
   * @param start Position in the output object array to start populating.
   * @param event The input event.
   * @param attributeDescs List of attribute descriptors to populate.
   * @param values Output value object array.
   * @return Next position in the output array.
   * @throws ApplicationException When an unexpected error happens.
   */
  public static int populateIngestValues(
      final int start,
      final Event event,
      final List<AttributeDesc> attributeDescs,
      final Object[] values)
      throws ApplicationException {
    int index = start;
    for (final AttributeDesc attributeDesc : attributeDescs) {
      final String key = attributeDesc.getName();
      Object value = event.get(key);
      if (value == null) {
        value = attributeDesc.getInternalDefaultValue();
      }
      values[index++] = dataEngineToCassandra(value, attributeDesc);
    }
    return index;
  }

  /**
   * Method to convert an attribute value of type for DataEngine to one for Cassandra.
   *
   * @param value Input value
   * @param attributeDesc Attribute descriptor
   * @return Converted attribute value
   * @throws ApplicationException When an unexpected error happens.
   */
  public static Object dataEngineToCassandra(final Object value, AttributeDesc attributeDesc)
      throws ApplicationException {
    if (attributeDesc == null) {
      throw new IllegalArgumentException("parameter attributeDesc may not be null");
    }
    if (value == null) {
      return null;
    }
    switch (attributeDesc.getAttributeType()) {
      case DATE:
        // Cassandra stores a date object as a com.datastax.driver.core.LocalDate
        // instance. TFOS internally uses Java LocalDate type so we convert here.
        return LocalDate.fromDaysSinceEpoch((int) ((java.time.LocalDate) value).toEpochDay());
      default:
        return value;
    }
  }

  /**
   * Method to convert an attribute value of type for Cassandra to one for Data Engine.
   *
   * @param value Input value
   * @param desc Attribute descriptor
   * @return Converted attribute value
   */
  public static Object cassandraToDataEngine(final Object value, final AttributeDesc desc) {
    if (desc == null) {
      throw new IllegalArgumentException("parameter desc may not be null");
    }
    switch (desc.getAttributeType()) {
      case DATE:
        // Cassandra stores a date object as a com.datastax.driver.core.LocalDate
        // instance. TFOS internally uses Java LocalDate type so we convert here.
        return java.time.LocalDate.ofEpochDay(((LocalDate) value).getDaysSinceEpoch());
      default:
        return value;
    }
  }

  /**
   * Method to summarize sequence of events.
   *
   * @param events Input events.
   * @param request Summarize request parameters
   * @param sortRequest Compiled sort request
   * @param mainCassStream CassStream used for grouping the events.
   * @param eventFactory Event factory used for generating an event.
   * @param isBiosApi True if the method is originally invoked by biOS API
   * @return Map of timestamp and list of corresponding summary entries.
   * @throws ApplicationException When an unexpected error happens.
   * @throws TfosException When a user error happens.
   */
  public Map<Long, List<Event>> summarizeEvents(
      List<Event> events,
      SummarizeRequest request,
      CompiledSortRequest sortRequest,
      CassStream mainCassStream,
      EventFactory eventFactory,
      boolean isBiosApi)
      throws ApplicationException, TfosException {
    // Do nothing in default
    return Collections.emptyMap();
  }

  // Table property access methods ////////////

  protected final String getProperty(String key) throws ApplicationException {
    if (propertiesImpl == null) {
      throw new ApplicationException("CassStreamProperties instance is not attached");
    }
    return propertiesImpl.getProperty(key);
  }

  protected final long getPropertyAsLong(String key, long defaultValue)
      throws ApplicationException {
    if (propertiesImpl == null) {
      throw new ApplicationException("CassStreamProperties instance is not attached");
    }
    return propertiesImpl.getPropertyAsLong(key, defaultValue);
  }

  protected final void setProperty(String key, String value) throws ApplicationException {
    if (propertiesImpl == null) {
      throw new ApplicationException("CassStreamProperties instance is not attached");
    }
    propertiesImpl.setProperty(key, value);
  }

  protected final void addProperties(Properties properties) throws ApplicationException {
    if (propertiesImpl == null) {
      throw new ApplicationException("CassStreamProperties instance is not attached");
    }
    propertiesImpl.addProperties(properties);
  }

  public static String generateTableName(StreamType streamType, String name, Long version) {
    return CassandraDataStoreUtils.generateTableName(
        getTablePrefixFromStreamType(streamType), name, version);
  }

  /**
   * Returns the table prefix given the stream type.
   *
   * @param streamType Stream type
   * @return Table prefix
   */
  public static String getTablePrefixFromStreamType(StreamType streamType) {
    String prefix;
    switch (streamType) {
      case SIGNAL:
        prefix = CassandraConstants.PREFIX_EVENTS_TABLE;
        break;
      case CONTEXT:
        prefix = CassandraConstants.PREFIX_CONTEXT_TABLE;
        break;
      case METRICS:
        prefix = CassandraConstants.PREFIX_METRICS_TABLE;
        break;
      case INDEX:
        prefix = CassandraConstants.PREFIX_INDEX_TABLE;
        break;
      case VIEW:
        prefix = CassandraConstants.PREFIX_VIEW_TABLE;
        break;
      case ROLLUP:
      case CONTEXT_FEATURE:
        prefix = CassandraConstants.PREFIX_ROLLUP_TABLE;
        break;
      case CONTEXT_INDEX:
        prefix = CassandraConstants.PREFIX_CONTEXT_INDEX_TABLE;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported StreamType: " + streamType);
    }
    return prefix;
  }

  public static String translateInvalidQueryMessage(String message) {
    return message
        .replace("evt_", "")
        .replace("of type text", "of type String")
        .replace("of type bigint", "of type Integer")
        .replace(" of type double", "of type Decimal")
        .replace(" of type boolean", "of type Boolean")
        .replace(" of type blob", "of type Blob");
  }

  protected int listSize(List<?> list) {
    return list != null ? list.size() : 0;
  }

  public long getTtlInTable() {
    if (defaultTtlInTable < 0) {
      final var metadata = cassandraConnection.getCluster().getMetadata();
      final var keyspaceMetadata = metadata.getKeyspace(keyspaceName);
      if (keyspaceMetadata != null) {
        final var tableMetadata = keyspaceMetadata.getTable(tableName);
        if (tableMetadata != null) {
          defaultTtlInTable = tableMetadata.getOptions().getDefaultTimeToLive();
        } else {
          logger.error("table not found");
        }
      }
    }
    return defaultTtlInTable * 1000;
  }
}
