// ============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
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
// ============================================================================

#ifndef DATAFRAME_SERIALIZER_RECORD_BATCH_FILE_HPP
#define DATAFRAME_SERIALIZER_RECORD_BATCH_FILE_HPP

#include <dataframe/serializer/base.hpp>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace dataframe {

class RecordBatchFileWriter : public Writer<RecordBatchFileWriter>
{
  public:
    std::size_t size() const
    {
        if (buffer_ == nullptr) {
            return 0;
        }

        return static_cast<std::size_t>(buffer_->size());
    }

    const std::uint8_t *data() const
    {
        if (buffer_ == nullptr) {
            return nullptr;
        }

        return buffer_->data();
    }

    void write(const DataFrame &df)
    {
        std::shared_ptr<::arrow::io::BufferOutputStream> stream;
        DF_ARROW_ERROR_HANDLER(::arrow::io::BufferOutputStream::Create(
            0, ::arrow::default_memory_pool(), &stream));
        std::shared_ptr<::arrow::ipc::RecordBatchWriter> writer;

        auto table = df.table();
        DF_ARROW_ERROR_HANDLER(::arrow::ipc::RecordBatchFileWriter::Open(
            stream.get(), table->schema(), &writer));
        DF_ARROW_ERROR_HANDLER(writer->WriteTable(*table));
        DF_ARROW_ERROR_HANDLER(writer->Close());
        DF_ARROW_ERROR_HANDLER(stream->Finish(&buffer_));
    }

  private:
    std::shared_ptr<::arrow::Buffer> buffer_;
};

class RecordBatchFileReader : public Reader<RecordBatchFileReader>
{
  public:
    DataFrame read_buffer(std::size_t n, const std::uint8_t *buf) const
    {
        auto buffer = std::make_shared<::arrow::Buffer>(
            buf, static_cast<std::int64_t>(n));

        ::arrow::io::BufferReader stream(buffer);
        std::shared_ptr<::arrow::ipc::RecordBatchFileReader> reader;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::ipc::RecordBatchFileReader::Open(&stream, &reader));

        auto nbatches = reader->num_record_batches();
        std::vector<std::shared_ptr<::arrow::RecordBatch>> batches(
            static_cast<std::size_t>(nbatches));
        for (auto i = 0; i != nbatches; ++i) {
            DF_ARROW_ERROR_HANDLER(reader->ReadRecordBatch(
                i, &batches[static_cast<std::size_t>(i)]));
        }

        std::shared_ptr<::arrow::Table> table;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::Table::FromRecordBatches(batches, &table));

        return DataFrame(table);
    }
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_RECORD_BATCH_FILE_HPP
