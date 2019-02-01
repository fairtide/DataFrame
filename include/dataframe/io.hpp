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

#ifndef DATAFRAME_IO_HPP
#define DATAFRAME_IO_HPP

#include <dataframe/error.hpp>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <fstream>
#include <set>

namespace dataframe {

enum class DataFormat { RecordBatchStream, RecordBatchFile, Feather };

namespace internal {

inline void convert(std::shared_ptr<::arrow::Buffer> &to,
    const ::arrow::Table &from,
    std::integral_constant<DataFormat, DataFormat::RecordBatchStream>)
{
    std::shared_ptr<::arrow::io::BufferOutputStream> stream;
    DF_ARROW_ERROR_HANDLER(::arrow::io::BufferOutputStream::Create(
        0, ::arrow::default_memory_pool(), &stream));
    std::shared_ptr<::arrow::ipc::RecordBatchWriter> writer;

    DF_ARROW_ERROR_HANDLER(::arrow::ipc::RecordBatchStreamWriter::Open(
        stream.get(), from.schema(), &writer));
    DF_ARROW_ERROR_HANDLER(writer->WriteTable(from));
    DF_ARROW_ERROR_HANDLER(writer->Close());
    DF_ARROW_ERROR_HANDLER(stream->Finish(&to));
}

inline void convert(std::shared_ptr<::arrow::Buffer> &to,
    const ::arrow::Table &from,
    std::integral_constant<DataFormat, DataFormat::RecordBatchFile>)
{
    std::shared_ptr<::arrow::io::BufferOutputStream> stream;
    DF_ARROW_ERROR_HANDLER(::arrow::io::BufferOutputStream::Create(
        0, ::arrow::default_memory_pool(), &stream));
    std::shared_ptr<::arrow::ipc::RecordBatchWriter> writer;

    DF_ARROW_ERROR_HANDLER(::arrow::ipc::RecordBatchFileWriter::Open(
        stream.get(), from.schema(), &writer));
    DF_ARROW_ERROR_HANDLER(writer->WriteTable(from));
    DF_ARROW_ERROR_HANDLER(writer->Close());
    DF_ARROW_ERROR_HANDLER(stream->Finish(&to));
}

inline void convert(std::shared_ptr<::arrow::Buffer> &to,
    const ::arrow::Table &from,
    std::integral_constant<DataFormat, DataFormat::Feather>)
{
    std::shared_ptr<::arrow::io::BufferOutputStream> stream;
    DF_ARROW_ERROR_HANDLER(::arrow::io::BufferOutputStream::Create(
        0, ::arrow::default_memory_pool(), &stream));

    std::unique_ptr<::arrow::ipc::feather::TableWriter> writer;
    DF_ARROW_ERROR_HANDLER(
        ::arrow::ipc::feather::TableWriter::Open(stream, &writer));
    writer->SetNumRows(from.num_rows());

    auto ncols = from.num_columns();
    for (auto i = 0; i != ncols; ++i) {
        auto col = from.column(i).get();
        auto chunks = col->data()->chunks();
        for (auto &&ary : chunks) {
            DF_ARROW_ERROR_HANDLER(writer->Append(col->field()->name(), *ary));
        }
    }

    DF_ARROW_ERROR_HANDLER(writer->Finalize());
    DF_ARROW_ERROR_HANDLER(stream->Finish(&to));
}

inline void convert(std::shared_ptr<::arrow::Table> &to,
    const ::arrow::Buffer &from,
    std::integral_constant<DataFormat, DataFormat::RecordBatchStream>)
{
    ::arrow::io::BufferReader stream(from);
    std::shared_ptr<::arrow::ipc::RecordBatchReader> reader;
    DF_ARROW_ERROR_HANDLER(
        ::arrow::ipc::RecordBatchStreamReader::Open(&stream, &reader));

    std::vector<std::shared_ptr<::arrow::RecordBatch>> batches;
    while (true) {
        std::shared_ptr<::arrow::RecordBatch> next;
        DF_ARROW_ERROR_HANDLER(reader->ReadNext(&next));
        if (next == nullptr) {
            break;
        }
        batches.push_back(std::move(next));
    }

    DF_ARROW_ERROR_HANDLER(::arrow::Table::FromRecordBatches(batches, &to));
}

inline void convert(std::shared_ptr<::arrow::Table> &to,
    const ::arrow::Buffer &from,
    std::integral_constant<DataFormat, DataFormat::RecordBatchFile>)
{
    ::arrow::io::BufferReader stream(from);
    std::shared_ptr<::arrow::ipc::RecordBatchFileReader> reader;
    DF_ARROW_ERROR_HANDLER(
        ::arrow::ipc::RecordBatchFileReader::Open(&stream, &reader));

    auto nbatches = reader->num_record_batches();
    std::vector<std::shared_ptr<::arrow::RecordBatch>> batches(
        static_cast<std::size_t>(nbatches));
    for (auto i = 0; i != nbatches; ++i) {
        DF_ARROW_ERROR_HANDLER(
            reader->ReadRecordBatch(i, &batches[static_cast<std::size_t>(i)]));
    }

    DF_ARROW_ERROR_HANDLER(::arrow::Table::FromRecordBatches(batches, &to));
}

inline void convert(std::shared_ptr<::arrow::Table> &to,
    const ::arrow::Buffer &from,
    std::integral_constant<DataFormat, DataFormat::Feather>)
{
    auto source = std::make_shared<::arrow::io::BufferReader>(from);
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

    to = ::arrow::Table::Make(
        std::make_shared<::arrow::Schema>(fields), columns, nrows);
}

} // namespace internal

template <DataFormat Format>
inline std::shared_ptr<::arrow::Table> deserialize(
    const ::arrow::Buffer &buffer)
{
    std::shared_ptr<::arrow::Table> ret;
    internal::convert(
        ret, buffer, std::integral_constant<DataFormat, Format>());

    return ret;
}

template <DataFormat Format>
inline std::shared_ptr<::arrow::Table> deserialize(
    std::size_t n, const std::uint8_t *buffer)
{
    return deserialize<Format>(
        ::arrow::Buffer(buffer, static_cast<std::int64_t>(n)));
}

template <DataFormat Format>
inline std::shared_ptr<::arrow::Buffer> serialize(const ::arrow::Table &table)
{
    std::shared_ptr<::arrow::Buffer> ret;
    internal::convert(
        ret, table, std::integral_constant<DataFormat, Format>());

    return ret;
}

inline std::shared_ptr<::arrow::Table> feather_read(const std::string &path)
{
    std::shared_ptr<::arrow::io::MemoryMappedFile> file;
    DF_ARROW_ERROR_HANDLER(::arrow::io::MemoryMappedFile::Open(
        path, ::arrow::io::FileMode::type::READ, &file));

    std::unique_ptr<::arrow::ipc::feather::TableReader> reader;
    DF_ARROW_ERROR_HANDLER(
        ::arrow::ipc::feather::TableReader::Open(file, &reader));

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

    return ::arrow::Table::Make(
        std::make_shared<::arrow::Schema>(fields), columns, nrows);
}

inline std::size_t feather_write(
    const std::string &path, const ::arrow::Table &table)
{
    auto buffer = serialize<DataFormat::Feather>(table);
    auto ret = static_cast<std::size_t>(buffer->size());

    std::ofstream out(path, std::ios::out | std::ios::binary);
    if (!out) {
        DataFrameException(
            "Failed to open " + path + " for writing feather file");
    }

    out.write(reinterpret_cast<const char *>(buffer->data()),
        static_cast<std::streamsize>(buffer->size()));
    if (!out) {
        DataFrameException("Failed to write feather file to  " + path);
    }

    return ret;
}

} // namespace dataframe

#endif // DATAFRAME_IO_HPP
