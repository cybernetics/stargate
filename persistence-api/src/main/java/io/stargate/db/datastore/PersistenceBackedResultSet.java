package io.stargate.db.datastore;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

class PersistenceBackedResultSet implements ResultSet {

  private final Persistence.Connection connection;
  private final Parameters parameters;
  // Can be null when we know there is a single page
  private final @Nullable Statement statement;
  private final ProtocolVersion driverProtocolVersion;
  private final Deque<Row> fetchedRows;
  private final List<Column> columns;

  // Paging state to fetch the next page, or null is we've fetched all pages.
  private ByteBuffer nextPagingState;

  PersistenceBackedResultSet(
      Persistence.Connection connection,
      Parameters parameters,
      @Nullable Statement statement,
      Result.Rows initialPage) {
    this.connection = connection;
    // We get our metadata in our initial page; let's skip it for following pages
    this.parameters = parameters.withoutMetadataInResult();
    this.statement = statement;
    this.driverProtocolVersion =
        PersistenceBackedDataStore.toDriverVersion(parameters.protocolVersion());
    this.fetchedRows = new ArrayDeque<>(parameters.pageSize().orElse(32));
    this.columns = processColumns(initialPage.resultMetadata.columns);
    processNewPage(initialPage);
    if (nextPagingState != null && this.statement == null) {
      throw new IllegalStateException(
          "The statement must be provided if there is more than the initial page.");
    }
  }

  static ResultSet create(
      Persistence.Connection connection,
      Result result,
      @Nullable BoundStatement statement,
      Parameters executeParameters) {
    switch (result.kind) {
      case Prepared:
        throw new AssertionError(
            "Shouldn't get a 'Prepared' result when executing a prepared statement");
      case SchemaChange:
        connection.persistence().waitForSchemaAgreement();
        return ResultSet.empty(true);
      case Void: // fallthrough on purpose
      case SetKeyspace:
        return ResultSet.empty();
      case Rows:
        return new PersistenceBackedResultSet(
            connection, executeParameters, statement, (Result.Rows) result);
      default:
        throw new AssertionError("Unhandled result type: " + result.kind);
    }
  }

  // We have the slight abstraction issue that the columns from Result.Rows "abuse" the Column
  // class a bit by returning instances that 1) may not represent genuine columns and 2) even when
  // they represents genuine column, maybe not not be "exactly" those column (in term of object
  // equality). See Result.Rows#columns javadoc for details.
  // Here, there is little we can do about non-genuine columns, but we can at least ensure that
  // for genuine columns, the object we use in the result set will full formed.
  private List<Column> processColumns(List<Column> columns) {
    Schema schema = connection.persistence().schema();
    List<Column> processed = new ArrayList<>(columns.size());
    for (Column c : columns) {
      Column inSchema = columnInSchema(schema, c);
      processed.add(inSchema == null ? c : inSchema);
    }
    return processed;
  }

  private static @Nullable Column columnInSchema(Schema schema, Column toFind) {
    Keyspace ks = schema.keyspace(toFind.keyspace());
    if (ks == null) return null;

    Table t = ks.table(toFind.table());
    if (t == null) return null;

    return t.column(toFind.name());
  }

  private void processNewPage(Result.Rows page) {
    for (List<ByteBuffer> rowValues : page.rows) {
      fetchedRows.addLast(new ArrayListBackedRow(columns, rowValues, driverProtocolVersion));
    }
    nextPagingState = page.resultMetadata.pagingState;
  }

  private void fetchNextPage() {
    assert nextPagingState != null;
    try {
      // Note: we could have add a timeout on that get() for security. That said, we don't want
      // to pull a random number, and adding a new config for that should probably be discussed.
      // But it's probably good enough to rely on the persistence layer query timeout.
      Result result =
          connection
              .execute(statement, parameters.withPagingState(nextPagingState), System.nanoTime())
              .get();

      switch (result.kind) {
        case Void:
          nextPagingState = null;
          break;
        case Rows:
          processNewPage((Result.Rows) result);
          break;
        default:
          throw new IllegalStateException(
              String.format("Unexpected %s result received for a result set page", result.kind));
      }
    } catch (InterruptedException e) {
      // We don't play with interruptions, so hopefully this never happen
      throw new RuntimeException("Interrupted while waiting on new page results");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // We only use unchecked exceptions, and we'd rather not wrap them as it makes it harder to
      // work with.
      throw (cause instanceof RuntimeException)
          ? ((RuntimeException) cause)
          : new RuntimeException(cause);
    }
  }

  private boolean hasNextRow() {
    while (true) {
      if (!fetchedRows.isEmpty()) {
        return true;
      }
      if (nextPagingState == null) {
        return false;
      }
      fetchNextPage();
    }
  }

  private Row nextRow() {
    while (true) {
      Row nextRow = fetchedRows.pollFirst();
      if (nextRow != null) {
        return nextRow;
      }
      if (nextPagingState == null) {
        throw new NoSuchElementException();
      }
      fetchNextPage();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return hasNextRow();
      }

      @Override
      public Row next() {
        return nextRow();
      }
    };
  }

  @Override
  public Row one() {
    return nextRow();
  }

  @Override
  public List<Row> rows() {
    List<Row> all = new ArrayList<>();
    while (hasNextRow()) {
      all.add(nextRow());
    }
    return all;
  }

  @Override
  public List<Row> currentPageRows() {
    List<Row> fetched = new ArrayList<>();
    while (!fetchedRows.isEmpty()) {
      fetched.add(fetchedRows.pollFirst());
    }
    return fetched;
  }

  @Override
  public boolean hasNoMoreFetchedRows() {
    return fetchedRows.isEmpty();
  }

  @Override
  public ByteBuffer getPagingState() {
    return nextPagingState;
  }

  @Override
  public boolean waitedForSchemaAgreement() {
    return false;
  }
}
