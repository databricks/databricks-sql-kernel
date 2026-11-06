// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Optionable as _;

use crate::Database;

#[derive(Debug, Default)]
pub struct Driver;

impl adbc_core::Driver for Driver {
    type DatabaseType = Database;

    fn new_database(&mut self) -> Result<Database> {
        Ok(Database::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Database> {
        let mut database = Database::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
