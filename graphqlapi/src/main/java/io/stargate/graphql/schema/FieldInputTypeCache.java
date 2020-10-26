package io.stargate.graphql.schema;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.types.MapBuilder;
import io.stargate.graphql.schema.types.TupleBuilder;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches GraphQL field input types, for example 'String' in:
 *
 * <pre>
 * input BooksInput {
 *   author: String
 *   title: String
 * }
 * </pre>
 *
 * These are used when receiving data, for in an 'insertBooks' mutation.
 */
class FieldInputTypeCache extends FieldTypeCache<GraphQLInputType> {

  private static final Logger LOG = LoggerFactory.getLogger(FieldInputTypeCache.class);

  FieldInputTypeCache(NameMapping nameMapping) {
    super(nameMapping);
  }

  @Override
  protected GraphQLInputType compute(Column.ColumnType columnType) {
    if (columnType.isMap()) {
      GraphQLType keyType = get(columnType.parameters().get(0));
      GraphQLType valueType = get(columnType.parameters().get(1));
      return new MapBuilder(keyType, valueType, true).build();
    } else if (columnType.isList() || columnType.isSet()) {
      return new GraphQLList(get(columnType.parameters().get(0)));
    } else if (columnType.isUserDefined()) {
      UserDefinedType udt = (UserDefinedType) columnType;
      return computeUdt(udt);
    } else if (columnType.isTuple()) {
      List<GraphQLType> subTypes =
          columnType.parameters().stream().map(this::get).collect(Collectors.toList());
      return new TupleBuilder(subTypes).buildInputType();
    } else {
      return getScalar(columnType.rawType());
    }
  }

  private GraphQLInputType computeUdt(UserDefinedType udt) {
    GraphQLInputObjectType.Builder builder =
        GraphQLInputObjectType.newInputObject().name(nameMapping.getGraphqlName(udt) + "Input");
    for (Column column : udt.columns()) {
      try {
        builder.field(
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getGraphqlName(udt, column))
                .type(get(column.type()))
                .build());
      } catch (Exception e) {
        // TODO find a better way to surface errors
        LOG.error(String.format("Input type for %s could not be created", column.name()), e);
      }
    }
    return builder.build();
  }
}
