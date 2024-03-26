#include "storage/postgres_type_entry.hpp"

namespace duckdb {

PostgresTypeEntry::PostgresTypeEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info,
                                     PostgresType postgres_type_p)
    : TypeCatalogEntry(catalog, schema, info), postgres_type(std::move(postgres_type_p)) {
}

} // namespace duckdb
