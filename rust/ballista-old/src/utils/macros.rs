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

/// Cast an Arrow Array to its expected type
#[macro_export]
macro_rules! cast_array {
    ($SELF:ident, $ARRAY_TYPE:ident) => {{
        match $SELF.as_any().downcast_ref::<array::$ARRAY_TYPE>() {
            Some(array) => Ok(array),
            None => Err(ballista_error("Failed to cast array to expected type")),
        }
    }};
}
