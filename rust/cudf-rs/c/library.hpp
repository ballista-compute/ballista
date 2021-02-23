// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef CUDF_C_LIBRARY_HPP
#define CUDF_C_LIBRARY_HPP

#include "rust/cxx.h"

#include <iostream>
#include <string>

#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>

#include <cudf/io/functions.hpp>
#include <cudf/io/types.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

class Table {
public:
    Table(std::unique_ptr<cudf::table> t);
    ~Table();

    std::unique_ptr<cudf::table> _table;
};

class TableView {
public:
    TableView(cudf::table_view t);
    ~TableView();

    cudf::table_view _table_view;
};

struct Aggregate;

std::unique_ptr<Table> read_parquet(rust::Str filename);

void write_parquet(rust::Str filename, std::unique_ptr<Table> table);

std::unique_ptr<Table> aggregate(
  std::unique_ptr<Table> table,
  rust::Vec<uint32_t> keys,
  rust::Vec<Aggregate> aggregate
);

#endif //CUDF_C_LIBRARY_HPP
