use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::crud::make::{Data, DATABASE};

pub mod crud;

enum Operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
}

enum LogicalOp {
    And,
    Or,
}

struct Condition {
    field: String,
    op: Operator,
    value: Data,
}

struct QueryBuilder<'a> {
    db: &'a DATABASE,
    table: String,
    conditions: Vec<(LogicalOp, Condition)>,
    limit: Option<usize>,
    sort_field: Option<String>,
    sort_ascending: bool,
}
impl<'a> QueryBuilder<'a> {
    pub fn new(db: &'a DATABASE, table: &str) -> Self {
        Self {
            db,
            table: table.to_string(),
            conditions: vec![],
            limit:Option::None,
            sort_field:Option::None,
            sort_ascending:true
        }
    }

    pub fn where_(
        mut self,
        field: &str,
        op: Operator,
        value: Data,
    ) -> Self {
        self.conditions.push((LogicalOp::And, Condition {
            field: field.to_string(),
            op,
            value,
        }));
        self
    }

    pub fn and(mut self, field: &str, op: Operator, value: Data) -> Self {
        self.conditions.push((LogicalOp::And, Condition {
            field: field.to_string(),
            op,
            value,
        }));
        self
    }

    pub fn or(mut self, field: &str, op: Operator, value: Data) -> Self {
        self.conditions.push((LogicalOp::Or, Condition {
            field: field.to_string(),
            op,
            value,
        }));
        self
    }
    pub fn filter(mut self, field: &str, op: Operator, value: Data) -> Self {
        let cond = Condition {
            field: field.to_string(),
            op,
            value,
        };
        self.conditions.push((LogicalOp::And, cond)); // default to AND
        self
    }
    pub fn execute(&self) -> Vec<HashMap<String, (Data, String)>> {
        let mut results = vec![];

        let mut path = PathBuf::from(&self.db.path);
        path.push(&self.table);

        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(data_str) = fs::read_to_string(entry.path()) {
                    let parsed: Result<HashMap<String, HashMap<String, (Data, String)>>, _> =
                        serde_json::from_str(&data_str);

                    if let Ok(map) = parsed {
                        for (_id, row) in map {
                            if self.matches_all(&row) {
                                results.push(row);
                            }
                        }
                    }
                }
            }
        }

        // Apply sorting if requested
        if let Some(field) = &self.sort_field {
            results.sort_by(|a, b| {
                let a_val = a.get(field);
                let b_val = b.get(field);

                // Compare Data values, handle None cases
                let ord = match (a_val, b_val) {
                    (Some((Data::NUMBER(a_num), _)), Some((Data::NUMBER(b_num), _))) => a_num.partial_cmp(b_num).unwrap_or(std::cmp::Ordering::Equal),
                    (Some((Data::STRING(a_str), _)), Some((Data::STRING(b_str), _))) => a_str.cmp(b_str),
                    _ => std::cmp::Ordering::Equal,
                };

                if self.sort_ascending {
                    ord
                } else {
                    ord.reverse()
                }
            });
        }

        // Apply limit if any
        if let Some(max) = self.limit {
            results.truncate(max);
        }

        results
    }

    // pub fn execute(&self) -> Vec<HashMap<String, (Data, String)>> {
    //     let mut results = vec![];
    //
    //     let mut path = PathBuf::from(&self.db.path);
    //     path.push(&self.table);
    //
    //     if let Ok(entries) = fs::read_dir(path) {
    //         for entry in entries.flatten() {
    //             if let Ok(data_str) = fs::read_to_string(entry.path()) {
    //                 let parsed: Result<HashMap<String, HashMap<String, (Data, String)>>, _> =
    //                     serde_json::from_str(&data_str);
    //
    //                 if let Ok(map) = parsed {
    //                     for (_id, row) in map {
    //                         if self.matches_all(&row) {
    //                             results.push(row);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //
    //     results
    // }

    fn matches_all(&self, row: &HashMap<String, (Data, String)>) -> bool {
        for (_, cond) in &self.conditions {
            match row.get(&cond.field) {
                Some((val, _)) => {
                    if !Self::compare(&cond.op, val.clone(), cond.value.clone()) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }

    fn compare(op: &Operator, left: Data, right: Data) -> bool {
        match (left, right) {
            (Data::STRING(a), Data::STRING(b)) => match op {
                Operator::Eq => a == b,
                Operator::Ne => a != b,
                Operator::Gt => a > b,
                Operator::Lt => a < b,
                Operator::Gte => a >= b,
                Operator::Lte => a <= b,
            },
            (Data::NUMBER(a), Data::NUMBER(b)) => match op {
                Operator::Eq => a == b,
                Operator::Ne => a != b,
                Operator::Gt => a > b,
                Operator::Lt => a < b,
                Operator::Gte => a >= b,
                Operator::Lte => a <= b,
            },
            _ => false, // Type mismatch
        }
    }

    pub fn select(&self) -> Vec<HashMap<String, (Data, String)>> {
        let table = self.db.get_table(&self.table).unwrap_or_default();
        table
            .into_iter()
            .filter(|(_, row)| self.matches_all(row))
            .map(|(_, row)| row)
            .collect()
    }

    pub fn update_row(self, new_row: HashMap<String, (Data, String)>) -> QueryBuilder<'a> {
        if let Some(table_data) = self.db.get_table(&self.table) {
            for (id, row) in table_data {
                if self.matches_all(&row) {
                    let _ = self.db.update_row_by_id(self.table.clone(), id, new_row.clone());
                }
            }
        }
        self
    }

    pub fn update_field(
        &self,
        fieldname: &str,
        new_value: (Data, String),
    ) -> &QueryBuilder<'a> {
        let mut updated_count = 0;
        let table = self.db.get_table(&self.table).unwrap_or_default();

        for (id, row) in table {
            if self.matches_all(&row) {
                self.db.update_field_by_id(
                    self.table.clone(),
                    id,
                    fieldname.to_string(),
                    new_value.clone(),
                );
                updated_count += 1;
            }
        }
        self
        // updated_count
    }

    pub fn insert(&self, table: &str, row: HashMap<String, (Data, String)>) -> Option<()> {
        self.db.add_row(table.to_string(), row, true).expect("203");
        Some(())
    }
    pub fn insert_many(&self, table: &str, row: Vec<HashMap<String, (Data, String)>>) -> Option<()> {
        self.db.add_rows(table.to_string(), row, true).expect("203");
        Some(())
    }

    pub fn count(&self) -> usize {
        self.execute().len()
    }

    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count);
        self
    }

    pub fn delete(self) -> QueryBuilder<'a> {
        if let Some(table_data) = self.db.get_table(&self.table) {
            for (id, row) in table_data {
                if self.matches_all(&row) {
                    let _ = self.db.delete_row_by_id(self.table.clone(), id);
                }
            }
        }
        self
    }

    pub fn first(self) -> Option<HashMap<String, (Data, String)>> {
        self.limit(1).execute().into_iter().next()
    }

    pub fn sort_by(mut self, field: &str, ascending: bool) -> Self {
        self.sort_field = Some(field.to_string());
        self.sort_ascending = ascending;
        self
    }

    pub fn exists(&self) -> bool {
        let mut path = PathBuf::from(&self.db.path);
        path.push(&self.table);

        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(data_str) = fs::read_to_string(entry.path()) {
                    let parsed: Result<HashMap<String, HashMap<String, (Data, String)>>, _> =
                        serde_json::from_str(&data_str);

                    if let Ok(map) = parsed {
                        for (_id, row) in map {
                            if self.matches_all(&row) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        false
    }

}
#[cfg(test)]
mod tests {
    use std::fs;

    use rand::Rng;

    use crate::crud::make::DATABASE;

    use super::*;

    #[cfg(test)]
    mod tests {
        use serde_json::{json, Number, Value};

        use super::*;

        use std::time::Instant;

        #[test]
        fn benchmark_add_column_migration() {
            let db = DATABASE::init("C:/Users/Milad/RustroverProjects/udb/benchmark_db".to_string());

            // Setup: create a table with 10,000 rows
            let start = Instant::now();
            db.generate_create_table_migration("create_people", "people","name", vec![
                ("name", "STRING"),
            ]).unwrap();
            db.apply_migrations().unwrap();
            let mut xx:Vec<HashMap<String,(Data,String)>> = vec![];
            for i in 0..100_000 {
                let mut x = HashMap::new();
                x.insert("name".to_string(),(Data::STRING(format!("user{}",i)),"".to_string()));
                // db.insert("people", hashmap! {
                xx.push(x);
                //     "name" => format!("User{}", i).into(),
                // }).unwrap();
            }
            db.query("people".to_string()).insert_many("people",xx);
            // rows.insert("id".to_string(),(Data::STRING("asd".to_string()),"".to_string()));
            //     rows.insert("years".to_string(),(Data::NUMBER(12 as f64), "".to_string()));
            println!("{:?}", start.elapsed());
            // Benchmark add_column
            let start = Instant::now();
            db.generate_add_column_migration("add_age", "people", "age", "NUMBER", Some(Value::from(Number::from(1)))).unwrap();
            db.apply_migrations().unwrap();
            let duration = start.elapsed();

            println!("⏱ Add column on 10,000 rows took: {:?}", duration);
        }
        // #[test]
        // fn test_full_migration_flow() {
        //     use std::fs;
        //     let db_path = "C:\\Users\\Milad\\RustroverProjects\\udb\\test_migration_db";
        //     // let _ = fs::remove_dir_all(db_path);
        //     let db = DATABASE::init(db_path.to_string());
        //
        //     // Migration 1: Create "people" table
        //     db.generate_create_table_migration(
        //         "create_people_table",
        //         "people",
        //         "id",
        //         vec![
        //             ("id", "STRING"),
        //             ("name", "STRING"),
        //         ],
        //     ).unwrap();
        //
        //     // Migration 2: Add "age" column
        //     db.generate_add_column_migration(
        //         "add_age_to_people",
        //         "people",
        //         "age",
        //         "NUMBER",
        //         Some(json!(0)), // Add a default value
        //     ).unwrap();
        //
        //     // Migration 3: Rename "age" to "years"
        //     db.generate_rename_column_migration(
        //         "people",
        //         "age",
        //         "years",
        //     ).unwrap();
        //
        //     // Migration 4: Drop "name"
        //     db.generate_drop_column_migration(
        //         "people",
        //         "name",
        //     ).unwrap();
        //
        //     // Migration 5: Delete "people" table
        //     // db.generate_delete_table_migration(
        //     //     "people"
        //     // ).unwrap();
        //
        //     // Apply all
        //     db.apply_migrations().unwrap();
        //
        //     // Validate .migrations_applied contains all
        //     let applied_file = format!("{}/migrations/.migrations_applied", db_path);
        //     let applied_content = fs::read_to_string(applied_file).unwrap();
        //     let applied_list: Vec<String> = serde_json::from_str(&applied_content).unwrap();
        //
        //     // assert_eq!(applied_list.len(), 5);
        //     // assert!(applied_list.contains(&"001_create_people_table.json".to_string()));
        //     // assert!(applied_list.contains(&"005_delete_people_table.json".to_string()));
        //
        //     // Check that the table no longer exists
        //     let people_path = format!("{}/people", db_path);
        //     // assert!(!std::path::Path::new(&people_path).exists());
        //     let mut rows = HashMap::new();
        //     rows.insert("id".to_string(),(Data::STRING("asd".to_string()),"".to_string()));
        //     rows.insert("years".to_string(),(Data::NUMBER(12 as f64), "".to_string()));
        //     let x = db.query("people".to_string()).insert("people",rows);
        //     // Clean up
        //     // let _ = fs::remove_dir_all(db_path);
        // }
        // }
    //     fn setup_db() -> DATABASE {
    //         // Create a fresh temporary directory or use a test folder path
    //         let test_path = "C:\\Users\\Milad\\RustroverProjects\\udb\\dbfiles";
    //             let table_name = "users";
    //             let id_field = "id";
    //         // Ideally, clear or recreate this directory at the start of each test
    //         let _ = std::fs::remove_dir_all(test_path);
    //         // std::fs::create_dir_all(test_path).unwrap();
    //
    //         DATABASE::init(test_path.to_string())
    //     }
    //
    //     #[test]
    //     fn test_crud_and_queries() {
    //         let db = setup_db();
    //
    //         let table_name = "users".to_string();
    //
    //         let mut fields = HashMap::new();
    //             fields.insert("id".to_string(), (Type::STRING, "".to_string()));
    //             fields.insert("name".to_string(), (Type::STRING, "".to_string()));
    //             fields.insert("age".to_string(), (Type::NUMBER, "".to_string()));
    //
    //             // Create table
    //             // db.create_table(fields, "id".to_string(), "users".to_string())?;
    //         //
    //         db.create_table(fields,"id".to_string(),table_name.clone()).expect("TODO: panic message");
    //
    //         // Insert some rows
    //         let mut row1 = HashMap::new();
    //         row1.insert("id".to_string(), (Data::STRING("u1".to_string()), "".to_string()));
    //         row1.insert("name".to_string(), (Data::STRING("Alice".to_string()), "".to_string()));
    //         row1.insert("age".to_string(), (Data::NUMBER(30.0), "".to_string()));
    //
    //         let mut row2 = HashMap::new();
    //         row2.insert("id".to_string(), (Data::STRING("u2".to_string()), "".to_string()));
    //         row2.insert("name".to_string(), (Data::STRING("Bob".to_string()), "".to_string()));
    //         row2.insert("age".to_string(), (Data::NUMBER(25.0), "".to_string()));
    //
    //         // Insert rows
    //         db.add_row(table_name.clone(), row1.clone(), true).unwrap();
    //         db.add_row(table_name.clone(), row2.clone(), true).unwrap();
    //
    //         // Use QueryBuilder to select users where age > 26
    //         let results = db.query(table_name.clone())
    //             .where_("age", Operator::Gt, Data::NUMBER(26.0))
    //             .execute();
    //
    //         assert_eq!(results.len(), 1);
    //         assert_eq!(results[0].get("name").unwrap().0, Data::STRING("Alice".to_string()));
    //
    //         // Update Bob's age to 28
    //         let updated_field = (Data::NUMBER(28.0), "".to_string());
    //         db.update_field_where(table_name.clone(), "id".to_string(), Data::STRING("u2".to_string()), "age".to_string(), updated_field.clone(), false, CMP::EQUAL)
    //             .unwrap();
    //
    //         // Query again to confirm Bob's age updated
    //         let results = db.query(table_name.clone())
    //             .where_("age", Operator::Gt, Data::NUMBER(26.0))
    //             .execute();
    //
    //         // Now both Alice and Bob should be returned because both > 26
    //         assert_eq!(results.len(), 2);
    //
    //         // Test count()
    //         let count = db.query(table_name.clone())
    //             .where_("age", Operator::Gt, Data::NUMBER(20.0))
    //             .count();
    //
    //         assert_eq!(count, 2);
    //
    //         // Test first()
    //         let first = db.query(table_name.clone())
    //             .where_("age", Operator::Gt, Data::NUMBER(20.0))
    //             .first();
    //
    //         assert!(first.is_some());
    //
    //         // Test exists()
    //         let exists = db.query(table_name.clone())
    //             .where_("name", Operator::Eq, Data::STRING("Alice".to_string()))
    //             .exists();
    //
    //         assert!(exists);
    //
    //         // Test delete
    //         db.delete_row_by_id(table_name.clone(), "u1".to_string());
    //
    //         let results_after_delete = db.query(table_name.clone())
    //             .execute();
    //
    //         assert_eq!(results_after_delete.len(), 1);
    //         assert_eq!(results_after_delete[0].get("name").unwrap().0, Data::STRING("Bob".to_string()));
    //
    //         // Clean up
    //         // std::fs::remove_dir_all("./test_db").unwrap();
    //     }
    // }


    // #[test]
    // fn test_string_to_numerical_uuid_consistency() {
    //     let input = "example_string";
    //     let uuid1 = string_to_numerical_uuid(input);
    //     let uuid2 = string_to_numerical_uuid(input);
    //     assert_eq!(uuid1, uuid2, "UUIDs from same input must be equal");
    //
    //     let diff_input = "different_string";
    //     let uuid3 = string_to_numerical_uuid(diff_input);
    //     assert_ne!(uuid1, uuid3, "UUIDs from different inputs should differ");
    // }
    //
    // #[test]
    // fn test_data_getters() {
    //     let s = Data::STRING("hello".to_string());
    //     assert_eq!(s.clone().get_string(), "hello");
    //
    //     let n = Data::NUMBER(42.0);
    //     assert_eq!(n.clone().get_number(), 42.0);
    //
    //     let b = Data::BOOLEAN(true);
    //     assert_eq!(b.clone().get_boolean(), true);
    //
    //     let arr = Data::ARRAY(vec![Data::NUMBER(1.0), Data::NUMBER(2.0)]);
    //     let arr_val = arr.clone().get_array();
    //     assert_eq!(arr_val.len(), 2);
    //
    //     let sn = Data::STRINGNULL(Some("optional".to_string()));
    //     assert_eq!(sn.clone().get_stringnull(), Some("optional".to_string()));
    // }
    //
    // #[test]
    // #[should_panic(expected = "expected STRING or NUMBER but got different variant")]
    // fn test_data_get_string_panic() {
    //     let d = Data::BOOLEAN(true);
    //     d.get_string(); // should panic
    // }
    //
    // #[test]
    // fn test_data_eq_type() {
    //     assert!(data_eq_type(&Data::STRING("x".to_string()), &Type::STRING));
    //     assert!(!data_eq_type(&Data::STRING("x".to_string()), &Type::NUMBER));
    //     assert!(data_eq_type(&Data::NUMBER(10.0), &Type::NUMBER));
    // }
    //
    // #[test]
    // fn test_create_table_creates_files() -> eyre::Result<()> {
    //     // Create temporary directory for database path
    //     let temp = tempdir()?;
    //     let db_path = temp.path().to_str().unwrap().to_string();
    //
    //     let db = DATABASE::init(db_path.clone());
    //
    //     // Define fields with id column
    //     let mut fields = HashMap::new();
    //     fields.insert("id".to_string(), (Type::STRING, "Primary key".to_string()));
    //     fields.insert("name".to_string(), (Type::STRINGNULL, "Optional name".to_string()));
    //
    //     // Create table
    //     db.create_table(fields, "id".to_string(), "users".to_string())?;
    //
    //     // Check table directory exists
    //     let mut table_dir = std::path::PathBuf::from(&db_path);
    //     table_dir.push("users");
    //     assert!(table_dir.exists());
    //
    //     // Check shard file exists
    //     let shard_file = table_dir.join("000000000000000000000000-000000000000000000000999.txt");
    //     assert!(shard_file.exists());
    //
    //     // Check schema file exists
    //     let schema_file = std::path::PathBuf::from(&db_path).join("users-type.txt");
    //     assert!(schema_file.exists());
    //
    //     Ok(())
    // }
    //
    // #[test]
    // #[should_panic(expected = "Id column")]
    // fn test_create_table_fails_missing_id() {
    //     let temp = tempdir().unwrap();
    //     let db_path = temp.path().to_str().unwrap().to_string();
    //     let db = DATABASE::init(db_path);
    //
    //     let fields = HashMap::new();
    //
    //     // Should panic because "id" field missing
    //     db.create_table(fields, "id".to_string(), "users".to_string())
    //         .unwrap();
    // }
    // //crud
    // #[test]
    // fn test_crud_flow() {
    //     use crate::crud::make::DATABASE;
    //     use crud::{c, r, u, d};
    //     let test_path = "C:\\Users\\Milad\\RustroverProjects\\udb\\dbfiles";
    //     let table_name = "users";
    //     let id_field = "id";
    //
    //     // Clean any previous test data
    //     let _ = fs::remove_dir_all(test_path);
    //
    //     // 1. Create database and table
    //     let db = DATABASE::init(test_path.to_string());
    //
    //     let mut fields = HashMap::new();
    //     fields.insert("id".to_string(), (Type::STRING, "".to_string()));
    //     fields.insert("name".to_string(), (Type::STRING, "".to_string()));
    //     fields.insert("age".to_string(), (Type::NUMBER, "".to_string()));
    //
    //     db.clone().create_table(fields, id_field.to_string(), table_name.to_string()).unwrap();
    //
    //     // 2. Insert row
    //     let mut new_row = HashMap::new();
    //     new_row.insert("id".to_string(),(Data::STRING("u1".to_string()),"".to_string()));
    //     new_row.insert("name".to_string(),(Data::STRING("Alice".to_string()),"".to_string()));
    //     new_row.insert("age".to_string(),(Data::NUMBER(30.0),"".to_string()));
    //
    //     let mut new_row2 = HashMap::new();
    //     new_row2.insert("id".to_string(),(Data::STRING("u2".to_string()),"".to_string()));
    //     new_row2.insert("name".to_string(),(Data::STRING("Alice".to_string()),"".to_string()));
    //     new_row2.insert("age".to_string(),(Data::NUMBER(35.0),"".to_string()));
    //     db.add_row(table_name.to_string(), new_row.clone(),false).unwrap();
    //     db.add_row(table_name.to_string(), new_row2.clone(),false).unwrap();
    //     // db.add_row("t1".to_string(),HashMap::from([("name".to_string(),(Data::STRING(z.to_string()),"".to_string()))]));
    //
    //     // 3. Select and verify inserted row
    //     let result = db.get_where(table_name.to_string(), "name".to_string(),Data::STRING("Alice".to_string()),true,CMP::EQUAL);
    //     println!("{:?}", result);
    //     // assert_eq!(result.len(), 2);
    //     // assert_eq!(result.get(0).unwrap()., Some(&Data::NUMBER(30.0)));
    //
    //     // 4. Update row
    //     let mut updated_row = HashMap::new();
    //     updated_row.insert("age".to_string(),( Data::NUMBER(31.0),"".to_string()));
    //
    //     db.update_row_where(table_name.to_string(), "name".to_string(), Data::STRING("Alice".to_string()), updated_row, true, CMP::EQUAL);
    //
    //     // 5. Verify update
    //     // let result = db.get_where(table_name.to_string(), "name".to_string(),Data::STRING("Alice".to_string()),true,CMP::EQUAL);
    //     let result = db.get_where(table_name.to_string(), "name".to_string(),Data::STRING("Alice".to_string()),true,CMP::EQUAL);
    //     println!("age must be 31 {:?}", result);
    //     // assert_eq!(result.get("age"), Some(&Data::NUMBER(31.0)));
    //
    //     // 6. Delete row
    //     db.delete_row_where(table_name.to_string(), "name".to_string(),Data::STRING("Alice".to_string()),true,CMP::EQUAL);
    //
    //     // 7. Verify deletion
    //     let result = db.get_where(table_name.to_string(), "name".to_string(),Data::STRING("Alice".to_string()),true,CMP::EQUAL );
    //     assert_eq!(result.len(), 0); // Should return an error or None
    // }


    // #[test]
    // fn it_works() {
        // let h: HashMap<String, String> = HashMap::new();
        // let s = serde_json::to_string(&h).unwrap();
        // println!("{s}");


        // let db = DATABASE::init("C:\\Users\\Milad\\RustroverProjects\\udb\\dbfiles".to_string());
        // db.clone().update_row_where("t1".to_string(),"name".to_string(),Data::STRING("6856.905653915126".to_string()),HashMap::from([("name".to_string(),(Data::STRING("asdasd".to_string()),"".to_string()))]),true);
        // db.clone().update_row_by_id("t1".to_string(),"2178.3511531201684".to_string(),HashMap::from([("name".to_string(),(Data::STRING("qwerty".to_string()),"".to_string()))]));
        // db.clone().update_field_where("t1".to_string(),"name".to_string(),Data::STRING("3376.866146841456".to_string()),"name".to_string(),(Data::STRING("qwer".to_string()),"".to_string()),true);
        // db.clone().update_row_where("t1".to_string(),"name".to_string(),Data::STRING("5240.555482275452".to_string()),HashMap::from([("name".to_string(),(Data::STRING("21wert1".to_string()),"".to_string()))]),true,CMP::EQUAL);
        // let x = db.clone().get_all("t1".to_string());
        // println!("{:?}",x);
        // 19447009":{"name":[{"STRING":"6856.905653915126"},


        // loop {
        //     let mut rng = rand::thread_rng();
        //     let y: f64 = rng.random();
        //     let z = y * 10000.0;
        //     db.clone().add_row("t1".to_string(),HashMap::from([("name".to_string(),(Data::STRING(z.to_string()),"".to_string()))]));
        // }
        // TABLE::create_table("C:\\Users\\Milad\\RustroverProjects\\udb\\dbfiles\\".to_string(),HashMap::from([("name".to_string(),(Type::STRING,"pass".to_string()))]),"pass".to_string(),"t1".to_string()).unwrap();
        // PageIndex::new().add_page(1, "C:\\Users\\Milad\\RustroverProjects\\udb\\dbfiles\\".to_string());
        // let x = string_to_numerical_uuid("1asdfasdfasfdhhhdasdfasdfasdffasdf23");
        // println!("{x}");

    // }
    }
}
