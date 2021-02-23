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

#include "library.hpp"

#include "src/lib.rs.h"

#include <cudf/groupby.hpp>
#include <cudf/types.hpp>

Table::Table(std::unique_ptr<cudf::table> t)
        : _table(std::move(t)) {}

Table::~Table() {
}

TableView::TableView(cudf::table_view t)
        : _table_view(std::move(t)) {}

TableView::~TableView() {
}

std::unique_ptr<Table> read_parquet(rust::Str filename) {
    cudf::io::read_parquet_args read_args{cudf::io::source_info(std::string(filename))};
    cudf::io::table_with_metadata result = cudf::io::read_parquet(read_args);
    return std::make_unique<Table>(std::move(result.tbl));
}

void write_parquet(rust::Str filename, std::unique_ptr<Table> rust_table) {
    cudf::io::write_parquet_args out_args{cudf::io::sink_info{std::string(filename)}, rust_table->_table->view()};
    cudf::io::write_parquet(out_args);
}

std::unique_ptr<Table> aggregate(
        std::unique_ptr<Table> rust_table,
        rust::Vec<uint32_t> rust_keys,
        rust::Vec<Aggregate> rust_aggregate) {

    auto table_view = rust_table->_table->view();

    std::vector<int> key_indices;
    for (uint32_t i=0; i<rust_keys.size(); i++) {
        key_indices.push_back(rust_keys.data()[i]);
    }
    auto group_keys = table_view.select(key_indices);

    std::vector<cudf::groupby::aggregation_request> requests;
    for (uint32_t i=0; i<rust_aggregate.size(); i++) {
        auto aggr_def = rust_aggregate.data()[i];

        //TODO: map from expression name passed in, assume SUM for now
        auto aggr_expr = cudf::make_sum_aggregation();

        requests.emplace_back(cudf::groupby::aggregation_request());
        requests[i].values = table_view.column(aggr_def.value_index);
        requests[i].aggregations.push_back(std::move(aggr_expr));
    }

    //TODO: remove hard-coded assumptions
    cudf::groupby::groupby gb_obj(
            group_keys, cudf::null_policy::EXCLUDE, cudf::sorted::NO, {}, {});

    auto result = gb_obj.aggregate(requests);

//    cudf::table_view keys_view = result.first->view();
//
//    std::vector<std::unique_ptr<cudf::column>> cols;
//    for (uint32_t i=0; i<result.second.size(); i++) {
//        auto x = result.second[i];
//
//    }

//    cudf::table_view vals_view = result.second;  //cudf::table_view{result.second[0].results[0]->view()};
//    auto combined_view = cudf::table_view{{keys_view,vals_view}};

    //TODO this is just returning the group keys so far
    return std::make_unique<Table>(std::move(result.first));
}