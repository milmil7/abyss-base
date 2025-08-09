use std::cmp::{Ordering, PartialOrd};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use serde_json::{json, Value};
use std::io::Write;

use crate::crud::make::{Data, data_eq, data_eq_type, DATABASE, TABLE, Type};

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Data::*;
        if std::mem::discriminant(self) != std::mem::discriminant(other) {
            return None;
        }
        match (self, other) {
            (NULL, NULL) => None,
            (STRING(i), STRING(j)) => j.cmp(i).into(),
            (NUMBER(i), NUMBER(j)) => j.partial_cmp(i),
            (ARRAY(i), ARRAY(j)) => j.partial_cmp(i),
            (BOOLEAN(i), BOOLEAN(j)) => j.cmp(i).into(),
            (JSON(i), JSON(j)) => j.cmp(i).into(),
            (STRINGNULL(i), STRINGNULL(j)) => j.cmp(i).into(),
            (NUMBERNULL(i), NUMBERNULL(j)) => j.partial_cmp(i),
            (ARRAYNULL(i), ARRAYNULL(j)) => j.partial_cmp(i),
            (BOOLEANNULL(i), BOOLEANNULL(j)) => j.partial_cmp(i),
            (JSONNULL(i), JSONNULL(j)) => j.partial_cmp(i),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub enum CMP {
    EQUAL,
    LESS,
    LESSEQ,
    GREATER,
    GTEQ,
}

impl CMP {
    pub fn calculate(self, x: Data, y: Data) -> bool {
        match self {
            CMP::EQUAL => x == y,
            CMP::LESS => x < y,
            CMP::LESSEQ => x <= y,
            CMP::GREATER => x > y,
            CMP::GTEQ => x >= y,
        }
    }
}

impl DATABASE {
    fn next_migration_filename(&self, name: &str) -> Result<PathBuf, String> {
        let mut dir = PathBuf::from(&self.path);
        dir.push("migrations");

        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S%.6f").to_string();
        let filename = format!("{}_{}.json", timestamp, name);
        dir.push(filename);

        Ok(dir)
    }

    pub fn generate_rename_column_migration(
        &self,
        table: &str,
        old_field: &str,
        new_field: &str,
    ) -> Result<(), String> {
        let json = serde_json::json!({
        "operation": "rename_column",
        "table": table,
        "old_field": old_field,
        "new_field": new_field
    });

        let path = self.next_migration_filename("rename_column")?;
        fs::write(&path, serde_json::to_string_pretty(&json).unwrap())
            .map_err(|e| format!("Failed to write migration: {}", e))?;

        // println!("📝 Generated rename_column migration: {}", path.display());
        Ok(())
    }

    pub fn generate_drop_column_migration(
        &self,
        table: &str,
        field: &str,
    ) -> Result<(), String> {
        let json = serde_json::json!({
        "operation": "drop_column",
        "table": table,
        "field": field
    });

        let path = self.next_migration_filename("drop_column")?;
        fs::write(&path, serde_json::to_string_pretty(&json).unwrap())
            .map_err(|e| format!("Failed to write migration: {}", e))?;

        // println!("📝 Generated drop_column migration: {}", path.display());
        Ok(())
    }
    pub fn generate_delete_table_migration(
        &self,
        table: &str,
    ) -> Result<(), String> {
        let json = serde_json::json!({
        "operation": "delete_table",
        "table": table
    });

        let path = self.next_migration_filename("delete_table")?;
        fs::write(&path, serde_json::to_string_pretty(&json).unwrap())
            .map_err(|e| format!("Failed to write migration: {}", e))?;

        // println!("📝 Generated delete_table migration: {}", path.display());
        Ok(())
    }

    pub fn apply_migrations(&self) -> Result<(), String> {
        let mut applied = HashSet::new();
        let mut applied_path = PathBuf::from(&self.path);
        applied_path.push("migrations/.migrations_applied");

        // Load applied migrations
        if applied_path.exists() {
            let content = fs::read_to_string(&applied_path)
                .map_err(|e| format!("Failed to read applied migrations: {}", e))?;
            let parsed: Vec<String> = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse .migrations_applied: {}", e))?;
            applied.extend(parsed);
        }

        // List migration files
        let mut migrations_path = PathBuf::from(&self.path);
        migrations_path.push("migrations");

        let mut migrations: Vec<_> = fs::read_dir(&migrations_path)
            .map_err(|e| format!("Failed to read migrations dir: {}", e))?
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().map(|s| s == "json").unwrap_or(false))
            .collect();

        migrations.sort_by_key(|e| e.path());

        let mut newly_applied = vec![];

        for entry in migrations {
            let file_name = entry.file_name().into_string().unwrap();
            if applied.contains(&file_name) {
                continue;
            }

            let path = entry.path();
            let content = fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read migration {}: {}", file_name, e))?;

            let json: Value = serde_json::from_str(&content)
                .map_err(|e| format!("Invalid JSON in {}: {}", file_name, e))?;

            self.apply_migration(&json)?; // corrected to pass by reference

            newly_applied.push(file_name);
        }

        // Save updated .migrations_applied
        if !newly_applied.is_empty() {
            applied.extend(newly_applied.clone());
            let updated: Vec<_> = applied.into_iter().collect();
            let content = serde_json::to_string_pretty(&updated).unwrap();
            fs::write(applied_path, content)
                .map_err(|e| format!("Failed to write applied list: {}", e))?;
        }

        Ok(())
    }

    fn apply_migration(&self, migration: &Value) -> Result<(), String> {
        let op = migration["operation"].as_str().ok_or("Missing 'operation' field")?;
        let table = migration["table"].as_str().ok_or("Missing 'table' field")?;
        // println!("{}", migration);
        match op {
            "create_table" => {
                let id_column = migration["id_column"]
                    .as_str()
                    .ok_or("Missing 'id_column'")?
                    .to_string();

                let fields_obj = migration["fields"]
                    .as_object()
                    .ok_or("Missing or invalid 'fields' object")?;

                let mut fields = HashMap::new();

                for (field, value) in fields_obj {
                    let arr = value
                        .to_string();
                        // .ok_or(format!("Field '{}' must be an array", field))?;

                    // if arr.len() != 2 {
                    //     return Err(format!(
                    //         "Field '{}' must be a 2-element array like [\"STRING\", \"metadata\"]",
                    //         field
                    //     ));
                    // }

                    // let type_str = arr[0].as_str().ok_or("Invalid type string")?;
                    // let metadata_str = arr[1].as_str().ok_or("Invalid metadata string")?;
                    let type_str = arr.clone().replace("\"","");
                    let metadata_str = "";

                    let parsed_type = match type_str.as_str() {
                        "STRING" => Type::STRING,
                        "NUMBER" => Type::NUMBER,
                        "BOOLEAN" => Type::BOOLEAN,
                        _ => return Err(format!("Unsupported type '{}'", type_str)),
                    };

                    fields.insert(field.clone(), (parsed_type, metadata_str.to_string()));
                }

                self.create_table(fields, id_column, table.to_string()).unwrap();

                let mut schema_path = PathBuf::from(&self.path);
                schema_path.push(format!("{}-type.txt", table));

                let schema_str = fs::read_to_string(&schema_path).unwrap();
                let mut table: TABLE = serde_json::from_str(&schema_str).unwrap();
                self.save_schema(&table)?;

                // println!("✔️ Created table '{}'", table.name);
            }

            "add_column" => {
                let field = migration["field"].as_str().ok_or("Missing 'field' field")?;
                let datatype = migration["datatype"].as_str().ok_or("Missing 'datatype' field")?;
                let default = migration["default"].clone();

                let table_path = PathBuf::from(&self.path).join(table);
                // println!("123 {:?}", table_path);
                let entries = fs::read_dir(table_path).map_err(|e| e.to_string())?;

                for entry in entries.flatten() {
                    let path = entry.path();
                    let content = fs::read_to_string(&path).map_err(|e| e.to_string())?;
                    let mut map: HashMap<String, HashMap<String, (Data, String)>> =
                        serde_json::from_str(&content).map_err(|e| e.to_string())?;

                    for row in map.values_mut() {
                        if !row.contains_key(field) {
                            let data = match &default {
                                Value::String(s) => Data::STRING(s.clone()),
                                Value::Number(n) if n.is_f64() => Data::NUMBER(n.as_f64().unwrap()),
                                Value::Number(n) if n.is_i64() => Data::NUMBER(n.as_i64().unwrap() as f64),
                                Value::Bool(b) => Data::BOOLEAN(*b),
                                _ => return Err("Unsupported default value type".into()),
                            };

                            row.insert(field.to_string(), (data, datatype.to_string()));
                        }
                    }

                    let json = serde_json::to_string_pretty(&map).map_err(|e| e.to_string())?;
                    fs::write(&path, json).map_err(|e| e.to_string())?;
                }
                let mut schema_path = PathBuf::from(&self.path);
                schema_path.push(format!("{}-type.txt", table));

                let schema_str = fs::read_to_string(&schema_path)
                    .map_err(|e| format!("Failed to read schema: {}", e))?;
                let mut table: TABLE = serde_json::from_str(&schema_str)
                    .map_err(|e| format!("Failed to parse schema: {}", e))?;
                // ✅ Actually mutate the schema here!
                table.field_names.insert(field.to_string(), (Type::from_string(datatype.to_string()).unwrap(), String::new()));
                self.save_schema(&table).expect("TODO: panic message");
                drop(schema_str);
                // Save updated schema
                let mut schema_path = PathBuf::from(&self.path);
                schema_path.push(format!("{}-type.txt", table.name));

                let schema_str = fs::read_to_string(&schema_path)
                    .map_err(|e| format!("Failed to read schema: {}", e))?;
                let mut table: TABLE = serde_json::from_str(&schema_str)
                    .map_err(|e| format!("Failed to parse schema: {}", e))?;

                if table.id_column == field {
                    return Err("Cannot drop the ID field".to_string());
                }

                if table.field_names.remove(field).is_none() {
                    return Err(format!("Field '{}' not found in table '{}'", field, table.name));
                }

                // println!("🧩 Added column '{}' to table '{}'", field, table.name);
            }

            "rename_column" => {
                let table = migration["table"].as_str().ok_or("Missing table name")?;
                let old_field = migration["old_field"].as_str().ok_or("Missing old_field")?;
                let new_field = migration["new_field"].as_str().ok_or("Missing new_field")?;

                let schema_path = PathBuf::from(&self.path).join(format!("{}-type.txt", table));
                let schema_content = fs::read_to_string(&schema_path).map_err(|e| e.to_string())?;
                let schema: TABLE = serde_json::from_str(&schema_content).map_err(|e| e.to_string())?;

                if schema.id_column == old_field {
                    return Err("Cannot rename the id field of a table".into());
                }

                let table_path = PathBuf::from(&self.path).join(table);
                let entries = fs::read_dir(&table_path).map_err(|e| e.to_string())?;

                for entry in entries.flatten() {
                    let path = entry.path();
                    let content = fs::read_to_string(&path).map_err(|e| e.to_string())?;

                    let mut map: HashMap<String, HashMap<String, (Data, String)>> =
                        serde_json::from_str(&content).map_err(|e| e.to_string())?;

                    for row in map.values_mut() {
                        if let Some(value) = row.remove(old_field) {
                            row.insert(new_field.to_string(), value);
                        }
                    }

                    let json = serde_json::to_string_pretty(&map).map_err(|e| e.to_string())?;
                    fs::write(&path, json).map_err(|e| e.to_string())?;
                }
                let schema_str = fs::read_to_string(&schema_path)
                    .map_err(|e| format!("Failed to read schema: {}", e))?;
                let mut table: TABLE = serde_json::from_str(&schema_str)
                    .map_err(|e| format!("Failed to parse schema: {}", e))?;

                if let Some((ty, description)) = table.field_names.remove(old_field) {
                    table.field_names.insert(new_field.to_string(), (ty, description));
                } else {
                    return Err(format!("Field '{}' does not exist in table '{}'", old_field, table.name));
                }

                if table.id_column == old_field {
                    table.id_column = new_field.to_string();
                };

                self.save_schema(&table);
            }

            "drop_column" => {
                let table = migration["table"].as_str().ok_or("Missing table name")?;
                let field = migration["field"].as_str().ok_or("Missing field name")?;

                let table_path = PathBuf::from(&self.path).join(table);
                let entries = fs::read_dir(&table_path).map_err(|e| e.to_string())?;

                let schema_path = PathBuf::from(&self.path).join(format!("{}-type.txt", table));
                let schema_content = fs::read_to_string(&schema_path).map_err(|e| e.to_string())?;
                let schema: TABLE = serde_json::from_str(&schema_content).map_err(|e| e.to_string())?;

                if schema.id_column == field {
                    return Err("Cannot drop the id field of a table".into());
                }

                for entry in entries.flatten() {
                    let path = entry.path();
                    let content = fs::read_to_string(&path).map_err(|e| e.to_string())?;

                    let mut map: HashMap<String, HashMap<String, (Data, String)>> =
                        serde_json::from_str(&content).map_err(|e| e.to_string())?;

                    for row in map.values_mut() {
                        row.remove(field);
                    }

                    let json = serde_json::to_string_pretty(&map).map_err(|e| e.to_string())?;
                    fs::write(&path, json).map_err(|e| e.to_string())?;
                }

                // println!("❌ Dropped column '{}' from table '{}'", field, table);
                let mut schema_path = PathBuf::from(&self.path);
                schema_path.push(format!("{}-type.txt", table));

                let schema_str = fs::read_to_string(&schema_path).unwrap();
                let mut table: TABLE = serde_json::from_str(&schema_str).unwrap();
                let x = table.field_names.remove(field);
                if x.is_none() {
                    return Err(format!("Field '{}' not found in table '{}'", field, table.name));
                }
                self.save_schema(&table)?;
            }

            "delete_table" => {
                let table = migration["table"].as_str().ok_or("Missing table name")?;
                let table_path = PathBuf::from(&self.path).join(table);

                if table_path.exists() {
                    fs::remove_dir_all(&table_path).map_err(|e| e.to_string())?;
                    // println!("🗑️ Deleted table '{}'", table);
                } else {
                    // println!("⚠️ Table '{}' does not exist", table);
                }
                let mut schema_path = PathBuf::from(&self.path);
                schema_path.push(format!("{}-type.txt", table));

                let schema_str = fs::read_to_string(&schema_path).unwrap();
                let mut table: TABLE = serde_json::from_str(&schema_str).unwrap();
                self.save_schema(&table)?;
            }

            _ => return Err(format!("Unsupported operation: {}", op)),
        }

        Ok(())
    }

    fn save_schema(&self, table: &TABLE) -> Result<(), String> {
        let mut path = PathBuf::from(&self.path);
        path.push(format!("{}-type.txt", table.name));
        fs::write(&path, serde_json::to_string_pretty(&table).unwrap())
            .map_err(|e| e.to_string())
    }


    pub fn create_migration(&self, name: &str, content: &serde_json::Value) -> Result<(), String> {
        let mut migrations_path = PathBuf::from(&self.path);
        migrations_path.push("migrations");
        fs::create_dir_all(&migrations_path)
            .map_err(|e| format!("Failed to create migrations directory: {}", e))?;

        // Determine next migration number
        let mut max_number = 0;
        for entry in fs::read_dir(&migrations_path).map_err(|e| e.to_string())? {
            if let Ok(entry) = entry {
                if let Some(filename) = entry.file_name().to_str() {
                    if let Some(number) = filename.split('_').next() {
                        if let Ok(num) = number.parse::<u32>() {
                            max_number = max_number.max(num);
                        }
                    }
                }
            }
        }

        let next_number = format!("{:03}", max_number + 1);
        let safe_name = name.replace(' ', "_").to_lowercase();
        let filename = format!("{}_{}.json", next_number, safe_name);

        let mut file_path = migrations_path.clone();
        file_path.push(&filename);

        // println!("{:?}", file_path);
        // println!("{:?}", filename);
        // println!("{:?}", name);
        // println!("{:?}", next_number);
        // println!("{:?}", safe_name);
        File::create(&name).unwrap();
        let json_string = serde_json::to_string_pretty(content)
            .map_err(|e| format!("Failed to serialize migration JSON: {}", e))?;

        fs::write(&name, json_string)
            .map_err(|e| format!("Failed to write migration file: {}", e))?;

        // println!("✅ Created migration: {}", filename);
        Ok(())
    }

    fn get_applied_migrations(&self) -> Result<Vec<String>, String> {
        let mut path = PathBuf::from(&self.path);
        path.push("dbfiles/migrations/.migrations_applied");

        let contents = fs::read_to_string(path).map_err(|e| e.to_string())?;
        Ok(contents.lines().map(|s| s.trim().to_string()).collect())
    }

    fn mark_migration_applied(&self, file_name: &str) -> Result<(), String> {
        let mut path = PathBuf::from(&self.path);
        path.push("migrations/.migrations_applied");

        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map_err(|e| e.to_string())?;

        writeln!(file, "{}", file_name).map_err(|e| e.to_string())
    }

    pub fn generate_create_table_migration(
        &self,
        name: &str,
        table_name: &str,
        id_column: &str,
        fields: Vec<(&str, &str)>,
    ) -> Result<(), String> {
        let mut field_map = serde_json::Map::new();
        for (key, val) in fields {
            field_map.insert(key.to_string(), Value::String(val.to_string()));
        }

        let content = json!({
        "operation": "create_table",
        "table": table_name,
        "id_column": id_column,
        "fields": field_map
    });

        let filename = self.next_migration_filename(name)?;
        // println!("qwe {:?}", filename);
        self.create_migration(&filename.to_str().unwrap(), &content)
    }

    pub fn generate_add_column_migration(
        &self,
        name: &str,
        table: &str,
        field: &str,
        datatype: &str,
        default: Option<Value>,
    ) -> Result<(), String> {
        let mut content = json!({
        "operation": "add_column",
        "table": table,
        "field": field,
        "datatype": datatype
    });

        if let Some(def) = default {
            content["default"] = def;
        }

        let filename = self.next_migration_filename(name)?;
        self.create_migration(&filename.to_str().unwrap(), &content)
    }

    pub fn update_row_where(
        &self,
        tablename: String,
        fieldname: String,
        fieldvalue: Data,
        new_row: HashMap<String, (Data, String)>,
        multi: bool,
        cmp: CMP,
    ) -> Option<HashMap<String, (Data, String)>> {
        let mut path = PathBuf::from(&self.path);
        path.push(&tablename);
        let table_type = Self::get_type_file(tablename.clone(), self.path.clone());

        if table_type.id_column == fieldname {
            return self.update_row_by_id(tablename, fieldvalue.get_string(), new_row);
        }

        let ents = fs::read_dir(path).ok()?;
        for x in ents {
            let entry = x.ok()?.path();
            let data_str = fs::read_to_string(&entry).ok()?;
            let mut deser: HashMap<String, HashMap<String, (Data, String)>> =
                serde_json::from_str(&data_str).ok()?;

            for (key, mut record) in deser.clone() {
                if let Some((value, _)) = record.get(&fieldname) {
                    if cmp.clone().calculate(fieldvalue.clone(), value.clone()) {
                        // Merge new_row into existing record
                        for (k, v) in new_row.iter() {
                            record.insert(k.clone(), v.clone());
                        }

                        // Replace the row with the merged record
                        deser.insert(key.clone(), record.clone());

                        let seri = serde_json::to_string(&deser).ok()?;
                        let filename = Self::get_file_by_id(key.clone());
                        let mut new_path = PathBuf::from(&self.path);
                        new_path.push(&tablename);
                        new_path.push(filename);
                        if !new_path.exists() {
                            return None;
                        }
                        fs::write(new_path, seri).ok()?;

                        if !multi {
                            return Some(record);
                        }
                    }
                }
            }
        }

        Some(new_row)
    }

    pub fn update_field_where(
        &self,
        tablename: String,
        fieldname: String,
        fieldvalue: Data,
        field_to_change: String,
        new_field_val: (Data, String),
        multi: bool,
        cmp: CMP,
    ) -> Option<(Data, String)> {
        let mut path = PathBuf::from(&self.path);
        path.push(&tablename);
        let table_type = Self::get_type_file(tablename.clone(), self.path.clone());

        if table_type.id_column == field_to_change {
            return self.update_field_by_id(
                tablename,
                fieldvalue.get_string(),
                fieldname,
                new_field_val,
            );
        }

        let ents = fs::read_dir(path).ok()?;
        for x in ents {
            let t = x.ok()?.path();
            let data_str = fs::read_to_string(&t).ok()?;
            let mut deser: HashMap<String, HashMap<String, (Data, String)>> =
                serde_json::from_str(&data_str).ok()?;

            for (id, mut record) in deser.clone() {
                if let Some((val, _)) = record.get(&fieldname) {
                    if cmp.clone().calculate(fieldvalue.clone(), val.clone()) {
                        deser
                            .get_mut(&id)
                            .unwrap()
                            .insert(field_to_change.clone(), new_field_val.clone());
                        let seri = serde_json::to_string(&deser).ok()?;

                        let filename = Self::get_file_by_id(id.clone());
                        let mut new_path = PathBuf::from(&self.path);
                        new_path.push(&tablename);
                        new_path.push(filename);
                        if !new_path.exists() {
                            return None;
                        }
                        fs::write(new_path, seri).ok()?;

                        if !multi {
                            return Some(new_field_val);
                        }
                    }
                }
            }
        }

        Some(new_field_val)
    }

    pub fn update_row_by_id(
        &self,
        tablename: String,
        id_: String,
        new_row: HashMap<String, (Data, String)>,
    ) -> Option<HashMap<String, (Data, String)>> {
        let mut row = self.get_by_id(tablename.clone(), id_.clone())?;
        for (k, v) in new_row.iter() {
            row.insert(k.clone(), v.clone());
        }
        self.delete_row_by_id(tablename.clone(), id_.clone());
        self.add_row(tablename, row.clone(), true);
        Some(row)
    }

    pub fn update_field_by_id(
        &self,
        tablename: String,
        id_: String,
        fieldname: String,
        new_value: (Data, String),
    ) -> Option<(Data, String)> {
        let mut row = self.get_by_id(tablename.clone(), id_.clone())?;
        row.insert(fieldname, new_value.clone());
        self.delete_row_by_id(tablename.clone(), id_);
        self.add_row(tablename, row, true);
        Some(new_value)
    }

    pub fn get_type_file(table_name: String, path: String) -> TABLE {
        let filename = format!("{}-type.txt", table_name);
        let mut file_path = PathBuf::from(path);
        file_path.push(filename);
        let data = fs::read_to_string(file_path).unwrap();
        serde_json::from_str(&data).unwrap()
    }
}
