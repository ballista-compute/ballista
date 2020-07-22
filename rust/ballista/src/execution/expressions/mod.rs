// Copyright 2020 Andy Grove
//
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

//! Relational expressions that can be used in query plans.

pub use self::alias::{alias, aliased_aggr};
pub use self::arithmetic::{add, div, mult, subtract};
pub use self::avg::avg;
pub use self::column::col;
pub use self::comparison::compare;
pub use self::count::count;
pub use self::literal::lit;
pub use self::max::max;
pub use self::min::min;
pub use self::sum::sum;

mod alias;
mod arithmetic;
mod avg;
mod column;
mod comparison;
mod count;
mod literal;
mod max;
mod min;
mod sum;
