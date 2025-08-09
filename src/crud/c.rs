use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;

use eyre::{eyre, Result};
use num_bigint::BigUint;
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::crud::make::{Data, data_eq_type, DATABASE, TABLE, Type};

impl DATABASE {

    pub fn add_rows(
        &self,
        table_name: String,
        rows: Vec<HashMap<String, (Data, String)>>,
        overwrite: bool,
    ) -> Result<()> {
        // Load schema
        let mut type_path = PathBuf::from(&self.path);
        type_path.push(format!("{}-type.txt", table_name));
        let type_data = fs::read_to_string(&type_path)?;
        let table_schema: TABLE = serde_json::from_str(&type_data)?;

        // Map shard_filename -> Vec<(id, row)>
        let mut shard_batches: BTreeMap<String, Vec<(u128, HashMap<String, (Data, String)>)>> = BTreeMap::new();

        for row in rows {
            // Validate type
            if !Self::check_type_regex(&row, &table_schema)? {
                return Err(eyre!("Row data types or regex patterns do not match schema"));
            }

            // Extract ID
            let id_field = row.get(&table_schema.id_column)
                .ok_or_else(|| eyre!("Missing ID field '{}'", &table_schema.id_column))?;
            let id = Self::string_to_numerical_uuid(&id_field.0.clone().get_string());
            let (start, end) = Self::get_shard_range_(&id);
            let shard_file = format!("{}-{}.txt", start, end);

            // Queue into the shard file group
            shard_batches.entry(shard_file).or_default().push((id.parse()?, row));
        }

        // Now write each shard once
        let mut shard_path = PathBuf::from(&self.path);
        shard_path.push(&table_name);
        fs::create_dir_all(&shard_path)?; // Ensure folder exists

        for (shard_file, entries) in shard_batches {
            let mut path = shard_path.clone();
            path.push(shard_file);
            Self::add_many_to_file(path, entries, overwrite)?;
        }

        Ok(())
    }

    fn add_many_to_file(
        path: PathBuf,
        entries: Vec<(u128, HashMap<String, (Data, String)>)>,
        overwrite: bool,
    ) -> Result<()> {
        let mut map = if path.exists() {
            let content = fs::read_to_string(&path)?;
            serde_json::from_str(&content)?
        } else {
            HashMap::new()
        };

        for (id, row) in entries {
            if !overwrite && map.contains_key(&id.to_string()) {
                return Err(eyre!("Row with ID {} already exists", id));
            }
            map.insert(id.to_string(), row);
        }

        let json = serde_json::to_string_pretty(&map)?;
        fs::write(&path, json)?;
        Ok(())
    }



    pub fn add_row(&self, table_name: String, row: HashMap<String, (Data, String)>, overwrite: bool) -> Result<()> {
        let mut type_path = PathBuf::from(&self.path);
        type_path.push(format!("{}-type.txt", table_name));
        let type_data = fs::read_to_string(&type_path)?;
        let table_schema: TABLE = serde_json::from_str(&type_data)?;
        // println!("{:?}", row);
        if !Self::check_type_regex(&row, &table_schema)? {
            return Err(eyre!("Row data types or regex patterns do not match schema"));
        }

        let id_field = row.get(&table_schema.id_column)
            .ok_or_else(|| eyre!("Missing ID field '{}'", &table_schema.id_column))?;
        let id = Self::string_to_numerical_uuid(&id_field.0.clone().get_string());
        let (start, end) = Self::get_shard_range_(&id);
        let filename = format!("{}-{}.txt", start, end);

        let mut filepath = PathBuf::from(&self.path);
        filepath.push(&table_name);
        fs::create_dir_all(&filepath)?; // Ensure table folder exists
        filepath.push(&filename);

        Self::add_to_file(filepath, row, id, overwrite)
    }

    fn add_to_file(filepath: PathBuf, row: HashMap<String, (Data, String)>, id: String, overwrite: bool) -> Result<()> {
        let data: HashMap<String, HashMap<String, (Data, String)>> = if filepath.exists() {
            let content = fs::read_to_string(&filepath)?;
            serde_json::from_str(&content).unwrap_or_else(|_| HashMap::new())
        } else {
            HashMap::new()
        };

        let mut data = data;

        if data.contains_key(&id) && !overwrite {
            return Err(eyre!("ID '{}' already exists and overwrite is false", id));
        }

        data.insert(id, row);
        let serialized = serde_json::to_string(&data)?;
        fs::write(&filepath, serialized)?;
        Ok(())
    }

    pub fn string_to_numerical_uuid(input: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(input);
        let result = hasher.finalize();
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&result[..4]);
        let big_num = BigUint::from_bytes_be(&bytes);
        big_num.to_str_radix(10)
    }

    fn get_shard_range_(id: &str) -> (String, String) {
        let base = &id[..id.len().saturating_sub(7)];
        (format!("{}0000000", base), format!("{}9999999", base))
    }

    pub fn check_type_regex(row: &HashMap<String, (Data, String)>, types: &TABLE) -> Result<bool> {
        if row.len() != types.field_names.len() {
            return Ok(false);
        }

        for (field_name, (expected_type, regex_str)) in &types.field_names {
            let row_val = row.get(field_name).ok_or_else(|| eyre!("Missing field '{}'", field_name))?;
            let data = &row_val.0;
            let pattern = &row_val.1;

            if !data_eq_type(&data.clone(), &expected_type.clone()) {
                return Ok(false);
            }

            if !regex_str.is_empty() {
                let re = Regex::from_str(regex_str)?;
                if let Data::STRING(s) = data {
                    if !re.is_match(s) {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }
}
