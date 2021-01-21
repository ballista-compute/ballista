// Copyright 2021 Andy Grove
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

use std::fs::File;

use arrow::{error::Result, ipc::writer::FileWriter};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

/// Stream data to disk in Arrow IPC format

pub async fn write_stream_to_disk(stream: &mut SendableRecordBatchStream, path: &str) -> Result<()> {

    let file = File::create(&path)?;

    let mut writer = FileWriter::try_new(file, stream.schema().as_ref())?;

    while let Some(result) = stream.next().await {

        let batch = result?;

        writer.write(&batch)?;
    }

    writer.finish()
}
