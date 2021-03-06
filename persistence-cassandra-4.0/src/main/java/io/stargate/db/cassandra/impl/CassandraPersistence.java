package io.stargate.db.cassandra.impl;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import io.stargate.db.Authenticator;
import io.stargate.db.Batch;
import io.stargate.db.BoundStatement;
import io.stargate.db.ClientInfo;
import io.stargate.db.EventListener;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import io.stargate.db.cassandra.impl.interceptors.DefaultQueryInterceptor;
import io.stargate.db.cassandra.impl.interceptors.QueryInterceptor;
import io.stargate.db.datastore.common.AbstractCassandraPersistence;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.Message.Request;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraPersistence
    extends AbstractCassandraPersistence<
        Config,
        KeyspaceMetadata,
        TableMetadata,
        ColumnMetadata,
        UserType,
        IndexMetadata,
        ViewMetadata> {
  private static final Logger logger = LoggerFactory.getLogger(CassandraPersistence.class);

  /*
   * Initial schema migration can take greater than 2 * MigrationManager.MIGRATION_DELAY_IN_MS if a
   * live token owner doesn't become live within MigrationManager.MIGRATION_DELAY_IN_MS.
   */
  private static final int STARTUP_DELAY_MS =
      Integer.getInteger(
          "stargate.startup_delay_ms",
          3 * 60000); // MigrationManager.MIGRATION_DELAY_IN_MS is private

  private LocalAwareExecutorService executor;

  private CassandraDaemon daemon;
  private Authenticator authenticator;
  private QueryInterceptor interceptor;

  // C* listener that ensures that our Stargate schema remains up-to-date with the internal C* one.
  private SchemaChangeListener schemaChangeListener;

  public CassandraPersistence() {
    super("Apache Cassandra");
  }

  private StargateQueryHandler stargateHandler() {
    return (StargateQueryHandler) ClientState.getCQLQueryHandler();
  }

  @Override
  protected SchemaConverter newSchemaConverter() {
    return new SchemaConverter();
  }

  @Override
  protected Iterable<KeyspaceMetadata> currentInternalSchema() {
    return Iterables.transform(org.apache.cassandra.db.Keyspace.all(), Keyspace::getMetadata);
  }

  @Override
  protected void registerInternalSchemaListener(Runnable runOnSchemaChange) {
    schemaChangeListener =
        new SimpleCallbackMigrationListener() {
          @Override
          void onSchemaChange() {
            runOnSchemaChange.run();
          }
        };
    org.apache.cassandra.schema.Schema.instance.registerListener(schemaChangeListener);
  }

  @Override
  protected void unregisterInternalSchemaListener() {
    if (schemaChangeListener != null) {
      org.apache.cassandra.schema.Schema.instance.unregisterListener(schemaChangeListener);
    }
  }

  @Override
  protected void initializePersistence(Config config) {
    // C* picks this property during the static loading of the ClientState class. So we set it
    // early, to make sure that class is not loaded before we've set it.
    System.setProperty(
        "cassandra.custom_query_handler_class", StargateQueryHandler.class.getName());

    daemon = new CassandraDaemon(true);

    DatabaseDescriptor.daemonInitialization(() -> config);
    try {
      daemon.init(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable to start Cassandra persistence layer", e);
    }

    executor =
        SHARED.newExecutor(
            DatabaseDescriptor.getNativeTransportMaxThreads(),
            DatabaseDescriptor::setNativeTransportMaxThreads,
            Integer.MAX_VALUE,
            "transport",
            "Native-Transport-Requests");

    // Use special gossip state "X10" to differentiate stargate nodes
    Gossiper.instance.addLocalApplicationState(
        ApplicationState.X10, StorageService.instance.valueFactory.releaseVersion("stargate"));

    daemon.start();

    waitForSchema(STARTUP_DELAY_MS);

    authenticator = new AuthenticatorWrapper(DatabaseDescriptor.getAuthenticator());
    interceptor = new DefaultQueryInterceptor();

    interceptor.initialize();
    stargateHandler().register(interceptor);
  }

  @Override
  protected void destroyPersistence() {
    if (daemon != null) {
      daemon.deactivate();
      daemon = null;
    }
  }

  @Override
  public void registerEventListener(EventListener listener) {
    Schema.instance.registerListener(new EventListenerWrapper(listener));
    interceptor.register(listener);
  }

  @Override
  public ByteBuffer unsetValue() {
    return ByteBufferUtil.UNSET_BYTE_BUFFER;
  }

  @Override
  public Authenticator getAuthenticator() {
    return authenticator;
  }

  @Override
  public void setRpcReady(boolean status) {
    StorageService.instance.setRpcReady(status);
  }

  @Override
  public Connection newConnection(ClientInfo clientInfo) {
    return new CassandraConnection(clientInfo);
  }

  @Override
  public Connection newConnection() {
    return new CassandraConnection();
  }

  private <T extends Result> CompletableFuture<T> runOnExecutor(
      Supplier<T> supplier, boolean captureWarnings) {
    assert executor != null : "This persistence has not been initialized";
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.submit(
        () -> {
          if (captureWarnings) ClientWarn.instance.captureWarnings();
          try {
            @SuppressWarnings("unchecked")
            T resultWithWarnings =
                (T) supplier.get().setWarnings(ClientWarn.instance.getWarnings());
            future.complete(resultWithWarnings);
          } catch (Throwable t) {
            JVMStabilityInspector.inspectThrowable(t);
            PersistenceException pe =
                (t instanceof PersistenceException)
                    ? (PersistenceException) t
                    : Conversion.convertInternalException(t);
            pe.setWarnings(ClientWarn.instance.getWarnings());
            future.completeExceptionally(pe);
          } finally {
            // Note that it's a no-op if we haven't called captureWarnings
            ClientWarn.instance.resetWarnings();
          }
        });

    return future;
  }

  @Override
  public boolean isInSchemaAgreement() {
    // We only include live nodes because this method is mainly used to wait for schema
    // agreement, and waiting for failed nodes is not a great idea.
    // Also note that in theory getSchemaVersion can return null for some nodes, and if it does
    // the code below will likely return false (the null will be an element on its own), but that's
    // probably the right answer in that case. In practice, this shouldn't be a problem though.

    // Important: This must include all nodes including fat clients, otherwise we'll get write
    // errors
    // with INCOMPATIBLE_SCHEMA.
    return Gossiper.instance.getLiveMembers().stream()
            .filter(
                ep -> {
                  EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
                  return epState != null && !Gossiper.instance.isDeadState(epState);
                })
            .map(Gossiper.instance::getSchemaVersion)
            .collect(Collectors.toSet())
            .size()
        <= 1;
  }

  /**
   * When "cassandra.join_ring" is "false" {@link StorageService#initServer()} will not wait for
   * schema to propagate to the coordinator only node. This method fixes that limitation by waiting
   * for at least one backend ring member to become available and for their schemas to agree before
   * allowing initialization to continue.
   */
  private void waitForSchema(int delayMillis) {
    boolean isConnectedAndInAgreement = false;
    for (int i = 0; i < delayMillis; i += 1000) {
      if (Gossiper.instance.getLiveTokenOwners().size() > 0 && isInSchemaAgreement()) {
        logger.debug("current schema version: {}", Schema.instance.getVersion());
        isConnectedAndInAgreement = true;
        break;
      }

      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    if (!isConnectedAndInAgreement) {
      logger.warn(
          "Unable to connect to live token owner and/or reach schema agreement after {} milliseconds",
          delayMillis);
    }
  }

  private class CassandraConnection extends AbstractConnection {
    private final ClientState clientState;

    private CassandraConnection(@Nonnull ClientInfo clientInfo) {
      this(clientInfo, ClientState.forExternalCalls(clientInfo.remoteAddress()));
    }

    private CassandraConnection() {
      this(null, ClientState.forInternalCalls());
    }

    private CassandraConnection(@Nullable ClientInfo clientInfo, ClientState clientState) {
      super(clientInfo);
      this.clientState = clientState;

      if (!authenticator.requireAuthentication()) {
        clientState.login(AuthenticatedUser.ANONYMOUS_USER);
      }
    }

    @Override
    public Persistence persistence() {
      return CassandraPersistence.this;
    }

    @Override
    protected void loginInternally(io.stargate.db.AuthenticatedUser user) {
      try {
        clientState.login(new AuthenticatedUser(user.name()));
      } catch (AuthenticationException e) {
        throw new org.apache.cassandra.stargate.exceptions.AuthenticationException(e);
      }
    }

    @Override
    public Optional<String> usedKeyspace() {
      return Optional.ofNullable(clientState.getRawKeyspace());
    }

    private <T extends Result> CompletableFuture<T> executeRequestOnExecutor(
        Parameters parameters, long queryStartNanoTime, Supplier<Request> requestSupplier) {
      return runOnExecutor(
          () -> {
            QueryState queryState = new QueryState(clientState);
            Request request = requestSupplier.get();
            if (parameters.tracingRequested()) {
              ReflectionUtils.setTracingRequested(request);
            }
            request.setCustomPayload(parameters.customPayload().orElse(null));

            Message.Response response =
                ReflectionUtils.execute(request, queryState, queryStartNanoTime);

            // There is only 2 types of response that can come out: either a ResultMessage (which
            // itself can of different kind), or an ErrorMessage.
            if (response instanceof ErrorMessage) {
              // Note that we convert in runOnExecutor (to handle exceptions coming from other
              // parts of this method), but we need an unchecked exception here anyway, so
              // we convert, and runOnExecutor will detect it's already converted.
              throw Conversion.convertInternalException(
                  (Throwable) ((ErrorMessage) response).error);
            }

            @SuppressWarnings("unchecked")
            T result =
                (T)
                    Conversion.toResult(
                        (ResultMessage) response,
                        Conversion.toInternal(parameters.protocolVersion()));
            return result;
          },
          parameters.protocolVersion().isGreaterOrEqualTo(ProtocolVersion.V4));
    }

    @Override
    public CompletableFuture<Result> execute(
        Statement statement, Parameters parameters, long queryStartNanoTime) {
      return executeRequestOnExecutor(
          parameters,
          queryStartNanoTime,
          () -> {
            QueryOptions options =
                Conversion.toInternal(
                    statement.values(), statement.boundNames().orElse(null), parameters);

            if (statement instanceof SimpleStatement) {
              String queryString = ((SimpleStatement) statement).queryString();
              return new QueryMessage(queryString, options);
            } else {
              MD5Digest id = Conversion.toInternal(((BoundStatement) statement).preparedId());
              // The 'resultMetadataId' is a protocol v5 feature we don't yet support
              return new ExecuteMessage(id, null, options);
            }
          });
    }

    @Override
    public CompletableFuture<Result.Prepared> prepare(String query, Parameters parameters) {
      return executeRequestOnExecutor(
          parameters,
          // The queryStartNanoTime is not used by prepared message, so it doesn't really matter
          // that it's only computed now.
          System.nanoTime(),
          () -> new PrepareMessage(query, parameters.defaultKeyspace().orElse(null)));
    }

    @Override
    public CompletableFuture<Result> batch(
        Batch batch, Parameters parameters, long queryStartNanoTime) {
      return executeRequestOnExecutor(
          parameters,
          queryStartNanoTime,
          () -> {
            QueryOptions options = Conversion.toInternal(Collections.emptyList(), null, parameters);
            BatchStatement.Type internalBatchType = Conversion.toInternal(batch.type());
            List<Object> queryOrIdList = new ArrayList<>(batch.size());
            List<List<ByteBuffer>> allValues = new ArrayList<>(batch.size());

            for (Statement statement : batch.statements()) {
              queryOrIdList.add(queryOrId(statement));
              allValues.add(statement.values());
            }
            return new BatchMessage(internalBatchType, queryOrIdList, allValues, options);
          });
    }

    private Object queryOrId(Statement statement) {
      if (statement instanceof SimpleStatement) {
        return ((SimpleStatement) statement).queryString();
      } else {
        return Conversion.toInternal(((BoundStatement) statement).preparedId());
      }
    }
  }
}
