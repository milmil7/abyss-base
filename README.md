# AbyssBase: A Simple File-Based Database in Rust

AbyssBase is a lightweight, file-based database engine written in Rust. It provides basic CRUD (Create, Read, Update, Delete) operations, migrations, and a query builder, all using the filesystem for storage. AbyssBase is designed for learning, prototyping, and small-scale applications where a full database server is unnecessary.

---

## Features

- **File-based storage**: Data is stored in JSON files, sharded by ID for scalability.
- **Schema support**: Table schemas with type and regex validation.
- **CRUD operations**: Add, read, update, and delete rows.
- **Migrations**: Create, apply, and track schema migrations.
- **Query builder**: Chainable, expressive queries with filtering, sorting, and limits.
- **Type-safe data model**: Strongly typed data and schema definitions.

---

## Project Structure

- `crud/`
  - `c.rs` — Create (add) operations for rows and batch inserts.
  - `r.rs` — Read operations: get all, get by ID, and filtered queries.
  - `u.rs` — Update operations, migration generation, and application.
  - `d.rs` — Delete operations: by ID and by condition.
  - `make.rs` — Core data types, schema, and utility functions.
- `lib.rs` — Query builder, high-level API, and integration tests.
- `test_migration_db/` — Example migration and data files (for development/testing).

---

## Data Model

### `Data` Enum
Represents a value in a table row. Supported types:
- `NULL`, `STRING`, `NUMBER`, `ARRAY`, `BOOLEAN`, `JSON`
- Nullable variants: `STRINGNULL`, `NUMBERNULL`, etc.

### `Type` Enum
Defines the type of a field in a table schema.

### `TABLE` Struct
- `name`: Table name
- `id_column`: Primary key field
- `field_names`: Map of field name to (Type, regex pattern)

### `DATABASE` Struct
- `path`: Root directory for all data and schema files

---

## Usage

### 1. Initialize Database
```rust
let db = DATABASE::init("./dbfiles".to_string());
```

### 2. Create a Table
```rust
let mut fields = HashMap::new();
fields.insert("id".to_string(), (Type::STRING, "".to_string()));
fields.insert("name".to_string(), (Type::STRING, "".to_string()));
fields.insert("age".to_string(), (Type::NUMBER, "".to_string()));
db.create_table(fields, "id".to_string(), "users".to_string())?;
```

### 3. Insert Rows
```rust
let mut row = HashMap::new();
row.insert("id".to_string(), (Data::STRING("u1".to_string()), "".to_string()));
row.insert("name".to_string(), (Data::STRING("Alice".to_string()), "".to_string()));
row.insert("age".to_string(), (Data::NUMBER(30.0), "".to_string()));
db.insert("users", row);
```

### 4. Query Data
```rust
let results = db.query("users".to_string())
    .where_("age", Operator::Gt, Data::NUMBER(26.0))
    .execute();
```

### 5. Update Data
```rust
db.update_field_where(
    "users".to_string(),
    "id".to_string(),
    Data::STRING("u1".to_string()),
    "age".to_string(),
    (Data::NUMBER(31.0), "".to_string()),
    false,
    CMP::EQUAL,
);
```

### 6. Delete Data
```rust
db.delete_row_by_id("users".to_string(), "u1".to_string());
```

### 7. Migrations
- Generate migrations for schema changes (add/drop/rename columns, create/delete tables)
- Apply all pending migrations:
```rust
db.apply_migrations()?;
```

---

## Sharding and Storage
- Each table is a directory under the database path.
- Rows are sharded into files named by ID range (e.g., `000000000000000000000000-000000000000000000000999.txt`).
- Each file contains a JSON map of ID to row data.
- Table schemas are stored as `<table>-type.txt` in the root.
- Migrations are stored in `migrations/`.

---

## Query Builder Example
```rust
let results = db.query("users".to_string())
    .where_("age", Operator::Gt, Data::NUMBER(20.0))
    .and("name", Operator::Eq, Data::STRING("Alice".to_string()))
    .sort_by("age", true)
    .limit(10)
    .execute();
```

---

## Testing
- See `lib.rs` and `make.rs` for unit tests covering table creation, insertion, querying, and UUID generation.

---

## Extending
- Add new data types by extending the `Data` and `Type` enums.
- Implement new migrations in `u.rs`.
- Add new query operators in the `Operator` enum and `compare` function.

---

## License
MIT License. See main project for details.
