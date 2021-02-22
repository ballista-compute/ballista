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
