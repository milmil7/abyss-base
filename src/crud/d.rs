use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::crud::make::{Data, DATABASE};
use crate::crud::u::CMP;

impl DATABASE {
    pub fn delete_row_by_id(
        &self,
        tablename: String,
        id_: String,
    ) -> Option<HashMap<String, (Data, String)>> {
        let id = Self::string_to_numerical_uuid(&id_);
        let filename = Self::get_file_by_id(id.clone());

        let mut path = PathBuf::from(&self.path);
        path.push(&tablename);
        path.push(&filename);

        if !path.exists() {
            return None;
        }

        let data_str = fs::read_to_string(&path).ok()?;
        let mut deser: HashMap<String, HashMap<String, (Data, String)>> =
            serde_json::from_str(&data_str).ok()?;

        let got = deser.remove(&id);

        if let Some(_) = got {
            let str_new_data = serde_json::to_string(&deser).ok()?;
            fs::write(path, str_new_data).ok()?;
        }

        got
    }

    pub fn delete_row_where(
        &self,
        tablename: String,
        fieldname: String,
        fieldvalue: Data,
        multi: bool,
        cmp: CMP,
    ) {
        let mut path = PathBuf::from(&self.path);
        path.push(&tablename);

        let ents = match fs::read_dir(&path) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in ents.filter_map(Result::ok) {
            let file_path = entry.path();
            let data_str = match fs::read_to_string(&file_path) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let mut deser: HashMap<String, HashMap<String, (Data, String)>> =
                match serde_json::from_str(&data_str) {
                    Ok(d) => d,
                    Err(_) => continue,
                };

            let mut modified = false;

            let keys_to_remove: Vec<String> = deser
                .iter()
                .filter(|(_, row)| {
                    if let Some((val, _)) = row.get(&fieldname) {
                        cmp.clone().calculate(fieldvalue.clone(), val.clone())
                    } else {
                        false
                    }
                })
                .map(|(id, _)| id.clone())
                .collect();

            for id in keys_to_remove.iter() {
                deser.remove(id);
                modified = true;
                if !multi {
                    break;
                }
            }

            if modified {
                let updated_str = match serde_json::to_string(&deser) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                fs::write(file_path, updated_str).ok();
            }
        }
    }

    // Helper reused from previous code
    pub fn get_file_by_id(id: String) -> String {
        let mut start = id.clone();
        let mut end = id;
        for _ in 0..7 {
            start.pop();
            end.pop();
        }
        format!("{}0000000-{}9999999.txt", start, end)
    }
}
