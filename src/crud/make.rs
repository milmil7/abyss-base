use std::{fs, path::PathBuf};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use eyre::Result;
use crate::crud::u::CMP;
use crate::QueryBuilder;

#[derive(Clone)]
pub enum LogicOp {
    And(Box<Condition>),
    Or(Box<Condition>),
}

#[derive(Clone)]
pub struct Condition {
    pub field: String,
    pub cmp: CMP,
    pub value: Data,
    pub logic: Box<LogicOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DATABASE {
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TABLE {
    pub name: String,
    pub id_column: String,
    pub field_names: HashMap<String, (Type, String)>,
    // rows: HashMap<String, ROW>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Type {
    NULL,
    STRING,
    NUMBER,
    ARRAY,
    HASHMAP,
    BOOLEAN,
    JSON,
    HASHSET,
    TABLE,
    STRINGNULL,
    NUMBERNULL,
    ARRAYNULL,
    HASHMAPNULL,
    BOOLEANNULL,
    JSONNULL,
    HASHSETNULL,
    TABLENULL,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    NULL,
    STRING(String),
    NUMBER(f64),
    ARRAY(Vec<Data>),
    BOOLEAN(bool),
    JSON(String),
    STRINGNULL(Option<String>),
    NUMBERNULL(Option<f64>),
    ARRAYNULL(Option<Vec<Data>>),
    BOOLEANNULL(Option<bool>),
    JSONNULL(Option<String>),
}
impl Type {
    pub fn from_string(s:String) -> std::result::Result<Type, &'static str> {
        match s.as_str() {
            "NULL" => {return Ok(Type::NULL)}
            "STRING" => {return Ok(Type::STRING)}
            "NUMBER" => {return Ok(Type::NUMBER)}
            "ARRAY" => {return Ok(Type::ARRAY)}
            "HASHMAP" => {return Ok(Type::HASHMAP)}
            "BOOLEAN" => {return Ok(Type::BOOLEAN)}
            "JSON" => {return Ok(Type::JSON)}
            "HASHSET" => {return Ok(Type::HASHSET)}
            "TABLE" => {return Ok(Type::TABLE)}
            "STRINGNULL" => {return Ok(Type::STRINGNULL)}
            "NUMBERNULL" => {return Ok(Type::NUMBERNULL)}
            "ARRAYNULL" => {return Ok(Type::ARRAYNULL)}
            "HASHMAPNULL" => {return Ok(Type::HASHMAPNULL)}
            "BOOLEANNULL" => {return Ok(Type::BOOLEANNULL)}
            "JSONNULL" => {return Ok(Type::JSONNULL)}
            "HASHSETNULL" => {return Ok(Type::HASHSETNULL)}
            "TABLENULL" => {return Ok(Type::TABLENULL)}
            _ => return Err("No type name")
        }
    }
}
impl Data {
    pub fn get_string(self) -> String {
        match self {
            Data::STRING(x) => x,
            Data::NUMBER(x) => x.to_string(),
            _ => panic!("expected STRING or NUMBER but got different variant"),
        }
    }
    pub fn get_number(self) -> f64 {
        match self {
            Data::NUMBER(x) => x,
            _ => panic!("expected NUMBER but got different variant"),
        }
    }
    pub fn get_array(self) -> Vec<Data> {
        match self {
            Data::ARRAY(x) => x,
            _ => panic!("expected ARRAY but got different variant"),
        }
    }
    pub fn get_boolean(self) -> bool {
        match self {
            Data::BOOLEAN(x) => x,
            _ => panic!("expected BOOLEAN but got different variant"),
        }
    }
    pub fn get_json(self) -> String {
        match self {
            Data::JSON(x) => x,
            _ => panic!("expected JSON but got different variant"),
        }
    }
    pub fn get_stringnull(self) -> Option<String> {
        match self {
            Data::STRINGNULL(x) => x,
            _ => panic!("expected STRINGNULL but got different variant"),
        }
    }
    pub fn get_numbernull(self) -> Option<f64> {
        match self {
            Data::NUMBERNULL(x) => x,
            _ => panic!("expected NUMBERNULL but got different variant"),
        }
    }
    pub fn get_arraynull(self) -> Option<Vec<Data>> {
        match self {
            Data::ARRAYNULL(x) => x,
            _ => panic!("expected ARRAYNULL but got different variant"),
        }
    }
    pub fn get_booleannull(self) -> Option<bool> {
        match self {
            Data::BOOLEANNULL(x) => x,
            _ => panic!("expected BOOLEANNULL but got different variant"),
        }
    }
    pub fn get_jsonnull(self) -> Option<String> {
        match self {
            Data::JSONNULL(x) => x,
            _ => panic!("expected JSONNULL but got different variant"),
        }
    }
}

pub fn data_eq_type(x: &Data, y: &Type) -> bool {
    let x_type = match x {
        Data::NULL => Type::NULL,
        Data::STRING(_) => Type::STRING,
        Data::NUMBER(_) => Type::NUMBER,
        Data::ARRAY(_) => Type::ARRAY,
        Data::BOOLEAN(_) => Type::BOOLEAN,
        Data::JSON(_) => Type::JSON,
        Data::STRINGNULL(_) => Type::STRINGNULL,
        Data::NUMBERNULL(_) => Type::NUMBERNULL,
        Data::ARRAYNULL(_) => Type::ARRAYNULL,
        Data::BOOLEANNULL(_) => Type::BOOLEANNULL,
        Data::JSONNULL(_) => Type::JSONNULL,
    };

    &x_type == y
}

pub fn data_eq(x: &Data, y: &Data) -> bool {
    std::mem::discriminant(x) == std::mem::discriminant(y)
}

impl DATABASE {
    pub fn init(path: String) -> Self {
        let x = fs::exists(path.clone()).unwrap();
        if x {
            println!("1");
        }else {
            fs::create_dir(path.clone()).unwrap();
            fs::create_dir(format!("{}/migrations",path.clone())).unwrap();
            let mut migrations_applied = fs::File::create(format!("{}/migrations/.migrations_applied", path.clone())).unwrap();
            migrations_applied.write(b"[]").expect("174");
            println!("2 xr");
        };
        return Self { path }
    }

    pub fn query(&self, table_name: String) -> QueryBuilder {
        QueryBuilder::new(self, &table_name)
    }

    pub fn insert(&self, table: &str, row: HashMap<String, (Data, String)>) -> Option<()> {
        self.add_row(table.to_string(), row, true);
        Some(())
    }

    pub fn get_table(
        &self,
        table_name: &str,
    ) -> Option<HashMap<String, HashMap<String, (Data, String)>>> {
        let mut path = PathBuf::from(&self.path);
        path.push(table_name);
        let mut table = HashMap::new();

        for entry in fs::read_dir(path).ok()? {
            let entry = entry.ok()?;
            let file_str = fs::read_to_string(entry.path()).ok()?;
            let deser: HashMap<String, HashMap<String, (Data, String)>> =
                serde_json::from_str(&file_str).ok()?;
            for (id, row) in deser {
                table.insert(id, row);
            }
        }

        Some(table)
    }

    pub fn create_table(
        &self,
        fields: HashMap<String, (Type, String)>,
        id_field: String,
        name: String,
    ) -> Result<()> {
        // Check if id column exists
        if !fields.contains_key(&id_field) {
            eyre::bail!("Id column '{}' was not provided in fields", id_field);
        }

        let table = TABLE {
            name: name.clone(),
            id_column: id_field.clone(),
            field_names: fields.clone(),
        };

        // Create folder in database path for table if it doesn't exist
        let mut dir = PathBuf::from_str(&self.path)?;
        dir.push(&name);

        if !dir.exists() {
            fs::create_dir(&dir)?;

            // Create shard file placeholder
            dir.push("000000000000000000000000-000000000000000000000999.txt");
            File::create(&dir)?; // create empty file

            // Write empty hashmap JSON into the shard file
            let empty_map: HashMap<String, String> = HashMap::new();
            fs::write(&dir, serde_json::to_string(&empty_map)?)?;

            // Go back to database root path to create schema file
            dir.pop(); // remove shard filename
            dir.pop(); // remove table directory

            dir.push(format!("{}-type.txt", name));

            // Write serialized table schema to file
            fs::write(&dir, serde_json::to_string(&table)?)?;
        } else {
            eyre::bail!("Table '{}' already exists", name);
        }

        Ok(())
    }
}

pub fn string_to_numerical_uuid(input: &str) -> String {
    // Step 1: Hash the input string using SHA-256
    let mut hasher = Sha256::new();
    hasher.update(input);
    let result = hasher.finalize();

    // Step 2: Extract the first 10 bytes (80 bits) from the hash
    let mut bytes = [0u8; 10];
    bytes.copy_from_slice(&result[..10]);

    // Step 3: Convert bytes to BigUint
    let big_num = BigUint::from_bytes_be(&bytes);

    // Step 4: Return decimal string representation
    big_num.to_str_radix(10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = DATABASE::init(temp_dir.path().to_str().unwrap().to_string());

        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            (Type::STRING, "Primary key".to_string()),
        );
        fields.insert(
            "name".to_string(),
            (Type::STRINGNULL, "Nullable string".to_string()),
        );

        let result = db.create_table(fields, "id".to_string(), "users".to_string());
        assert!(result.is_ok());

        // Check files created
        let mut table_dir = temp_dir.path().to_path_buf();
        table_dir.push("users");
        assert!(table_dir.exists());

        let shard_file = table_dir.join("000000000000000000000000-000000000000000000000999.txt");
        assert!(shard_file.exists());

        let schema_file = temp_dir.path().join("users-type.txt");
        assert!(schema_file.exists());
    }

    #[test]
    #[should_panic(expected = "Id column")]
    fn test_create_table_missing_id_field() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = DATABASE::init(temp_dir.path().to_str().unwrap().to_string());

        let fields = HashMap::new();

        db.create_table(fields, "id".to_string(), "users".to_string())
            .unwrap();
    }

    #[test]
    fn test_string_to_numerical_uuid() {
        let uuid1 = string_to_numerical_uuid("example_string");
        let uuid2 = string_to_numerical_uuid("example_string");
        assert_eq!(uuid1, uuid2);

        let uuid3 = string_to_numerical_uuid("different_string");
        assert_ne!(uuid1, uuid3);
    }
}
