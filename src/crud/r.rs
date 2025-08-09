use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::crud::make::{Data, data_eq, data_eq_type, DATABASE};
use crate::crud::u::CMP;

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        if !data_eq(&self.clone(), &other.clone()) {
            return false;
        }

        match self {
            Data::NULL => true,
            Data::STRING(i) => i == &other.clone().get_string(),
            Data::NUMBER(i) => i == &other.clone().get_number(),
            Data::ARRAY(i) => i == &other.clone().get_array(),
            Data::BOOLEAN(i) => i == &other.clone().get_boolean(),
            Data::JSON(i) => i == &other.clone().get_json(),
            Data::STRINGNULL(i) => i == &other.clone().get_stringnull(),
            Data::NUMBERNULL(i) => i == &other.clone().get_numbernull(),
            Data::ARRAYNULL(i) => i == &other.clone().get_arraynull(),
            Data::BOOLEANNULL(i) => i == &other.clone().get_booleannull(),
            Data::JSONNULL(i) => i == &other.clone().get_jsonnull(),
        }
    }
}

impl DATABASE {
    pub fn get_all(&self, table_name: String) -> HashMap<String, HashMap<String, (Data, String)>> {
        let mut result = HashMap::new();
        let mut path = PathBuf::from(&self.path);
        path.push(&table_name);

        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(data_str) = fs::read_to_string(entry.path()) {
                    if let Ok(data) = serde_json::from_str::<HashMap<String, HashMap<String, (Data, String)>>>(&data_str) {
                        for (k, v) in data {
                            result.insert(k, v);
                        }
                    }
                }
            }
        }

        result
    }

    pub fn get_by_id(&self, table_name: String, id_input: String) -> Option<HashMap<String, (Data, String)>> {
        let id = Self::string_to_numerical_uuid(&id_input);
        let (start, end) = Self::get_shard_range(&id);
        let filename = format!("{}-{}.txt", start, end);

        let mut path = PathBuf::from(&self.path);
        path.push(table_name);
        path.push(filename);

        if !path.exists() {
            return None;
        }

        let data_str = fs::read_to_string(&path).ok()?;
        let deser: HashMap<String, HashMap<String, (Data, String)>> =
            serde_json::from_str(&data_str).ok()?;

        deser.get(&id).cloned()
    }

    pub fn get_where(
        &self,
        table_name: String,
        field_name: String,
        field_value: Data,
        multi: bool,
        cmp: CMP,
    ) -> Vec<(String, HashMap<String, (Data, String)>)> {
        let mut vec = vec![];
        let mut path = PathBuf::from(&self.path);
        path.push(table_name);

        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(data_str) = fs::read_to_string(entry.path()) {
                    if let Ok(deser) =
                        serde_json::from_str::<HashMap<String, HashMap<String, (Data, String)>>>(
                            &data_str,
                        )
                    {
                        for (id, row) in deser {
                            if let Some((data, _regex)) = row.get(&field_name) {
                                if cmp.clone().calculate(field_value.clone(), data.clone()) {
                                    vec.push((id, row));
                                    if !multi {
                                        return vec;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        vec
    }

    fn get_shard_range(id: &str) -> (String, String) {
        let base = &id[..id.len().saturating_sub(7)];
        (format!("{}0000000", base), format!("{}9999999", base))
    }
}
