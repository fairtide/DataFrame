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

#ifndef DATAFRAME_SERIALIZER_RECORD_BATCH_STREAM_HPP
#define DATAFRAME_SERIALIZER_RECORD_BATCH_STREAM_HPP

#include <dataframe/serializer/base.hpp>

namespace dataframe {

class RecordBatchStreamWriter : public Writer
{
  public:
    explicit RecordBatchStreamWriter(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
        : pool_(pool)
    {
    }

    std::size_t size() const override
    {
        if (buffer_ == nullptr) {
            return 0;
        }

        return static_cast<std::size_t>(buffer_->size());
    }

    const std::uint8_t *data() const override
    {
        if (buffer_ == nullptr) {
            return nullptr;
        }

        return buffer_->data();
    }

    void write(const DataFrame &df) override
    {
        if (df.empty()) {
            buffer_ = nullptr;
            return;
        }

        std::shared_ptr<::arrow::io::BufferOutputStream> stream;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::io::BufferOutputStream::Create(0, pool_, &stream));
        std::shared_ptr<::arrow::ipc::RecordBatchWriter> writer;

        DF_ARROW_ERROR_HANDLER(::arrow::ipc::RecordBatchStreamWriter::Open(
            stream.get(), df.table()->schema(), &writer));
        DF_ARROW_ERROR_HANDLER(writer->WriteTable(*df.table()));
        DF_ARROW_ERROR_HANDLER(writer->Close());
        DF_ARROW_ERROR_HANDLER(stream->Finish(&buffer_));
    }

    ::arrow::MemoryPool *memory_pool() const { return pool_; }

  private:
    ::arrow::MemoryPool *pool_;
    std::shared_ptr<::arrow::Buffer> buffer_;
};

class RecordBatchStreamReader : public Reader
{
  public:
    explicit RecordBatchStreamReader(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
        : pool_(pool)
    {
    }

    DataFrame read_buffer(
        std::size_t n, const std::uint8_t *buf, bool zero_copy) override
    {
        std::unique_ptr<::arrow::io::BufferReader> source;
        if (zero_copy) {
            source = std::make_unique<::arrow::io::BufferReader>(
                buf, static_cast<std::int64_t>(n));
        } else {
            source = std::make_unique<CopyBufferReader>(
                buf, static_cast<std::int64_t>(n), pool_);
        }

        std::shared_ptr<::arrow::ipc::RecordBatchReader> reader;
        DF_ARROW_ERROR_HANDLER(::arrow::ipc::RecordBatchStreamReader::Open(
            source.get(), &reader));

        std::vector<std::shared_ptr<::arrow::RecordBatch>> batches;
        while (true) {
            std::shared_ptr<::arrow::RecordBatch> next;
            DF_ARROW_ERROR_HANDLER(reader->ReadNext(&next));
            if (next == nullptr) {
                break;
            }
            batches.push_back(std::move(next));
        }

        std::shared_ptr<::arrow::Table> table;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::Table::FromRecordBatches(batches, &table));

        return DataFrame(std::move(table));
    }

    ::arrow::MemoryPool *memory_pool() const { return pool_; }

  private:
    ::arrow::MemoryPool *pool_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_RECORD_BATCH_STREAM_HPP
