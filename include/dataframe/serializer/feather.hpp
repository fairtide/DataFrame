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

#ifndef DATAFRAME_SERIALIZER_FEATHER_HPP
#define DATAFRAME_SERIALIZER_FEATHER_HPP

#include <dataframe/serializer/base.hpp>

namespace dataframe {

class FeatherWriter : public Writer
{
  public:
    std::size_t size() const final
    {
        if (buffer_ == nullptr) {
            return 0;
        }

        return static_cast<std::size_t>(buffer_->size());
    }

    const std::uint8_t *data() const final
    {
        if (buffer_ == nullptr) {
            return nullptr;
        }

        return buffer_->data();
    }

    void write(const DataFrame &df) final
    {
        if (df.empty()) {
            buffer_ = nullptr;
            return;
        }

        std::shared_ptr<::arrow::io::BufferOutputStream> stream;
        DF_ARROW_ERROR_HANDLER(::arrow::io::BufferOutputStream::Create(
            0, ::arrow::default_memory_pool(), &stream));

        auto &table = df.table();
        std::unique_ptr<::arrow::ipc::feather::TableWriter> writer;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::ipc::feather::TableWriter::Open(stream, &writer));
        writer->SetNumRows(table.num_rows());

        auto ncols = table.num_columns();
        for (auto i = 0; i != ncols; ++i) {
            auto col = table.column(i);
            auto chunks = col->data()->chunks();
            for (auto &&ary : chunks) {
                DF_ARROW_ERROR_HANDLER(
                    writer->Append(col->field()->name(), *ary));
            }
        }

        std::shared_ptr<::arrow::Buffer> ret;
        DF_ARROW_ERROR_HANDLER(writer->Finalize());
        DF_ARROW_ERROR_HANDLER(stream->Finish(&buffer_));
    }

  private:
    std::shared_ptr<::arrow::Buffer> buffer_;
};

class FeatherReader : public Reader
{
  public:
    DataFrame read_buffer(
        std::size_t n, const std::uint8_t *buf, bool zero_copy) final
    {
        std::shared_ptr<::arrow::io::BufferReader> source;
        if (zero_copy) {
            source = std::make_shared<::arrow::io::BufferReader>(
                buf, static_cast<std::int64_t>(n));
        } else {
            source = std::make_shared<CopyBufferReader>(
                buf, static_cast<std::int64_t>(n));
        }

        std::unique_ptr<::arrow::ipc::feather::TableReader> reader;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::ipc::feather::TableReader::Open(source, &reader));

        auto nrows = reader->num_rows();
        auto ncols = reader->num_columns();
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        std::vector<std::shared_ptr<::arrow::Column>> columns;
        for (auto i = 0; i != ncols; ++i) {
            std::shared_ptr<::arrow::Column> col;
            DF_ARROW_ERROR_HANDLER(reader->GetColumn(i, &col));
            fields.push_back(col->field());
            columns.push_back(std::move(col));
        }

        auto table = ::arrow::Table::Make(
            std::make_shared<::arrow::Schema>(fields), columns, nrows);

        return DataFrame(std::move(table));
    }
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_FEATHER_HPP
