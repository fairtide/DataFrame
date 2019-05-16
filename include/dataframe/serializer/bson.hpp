//============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//============================================================================

#ifndef DATAFRAME_SERIALIZER_BSON_HPP
#define DATAFRAME_SERIALIZER_BSON_HPP

#include <dataframe/serializer/base.hpp>
#include <dataframe/serializer/bson/column_reader.hpp>
#include <dataframe/serializer/bson/column_writer.hpp>

namespace dataframe {

class BSONWriter : public Writer
{
  public:
    BSONWriter(int compression_level = 0, bool ignore_float_na = true,
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
        : column_writer_(compression_level, ignore_float_na, pool)
        , pool_(pool)
    {
    }

    std::size_t size() const final
    {
        return data_ == nullptr ? 0 : data_->view().length();
    }

    const std::uint8_t *data() const final
    {
        return data_ == nullptr ? nullptr : data_->view().data();
    }

    void write(const DataFrame &data) override
    {
        ::bsoncxx::builder::basic::document builder;

        auto ncol = data.ncol();
        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = data[i];
            builder.append(::bsoncxx::builder::basic::kvp(
                col.name(), column_writer_.write(*col.data())));
        }

        data_ =
            std::make_unique<::bsoncxx::document::value>(builder.extract());
    }

    ::bsoncxx::document::value extract() { return std::move(*data_); }

    int compression_level() const
    {
        return column_writer_.compression_level();
    }

    ::arrow::MemoryPool *memory_pool() const { return pool_; }

  protected:
    bson::ColumnWriter column_writer_;
    ::arrow::MemoryPool *pool_;
    std::unique_ptr<::bsoncxx::document::value> data_;
};

class BSONReader : public Reader
{
  public:
    BSONReader(::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
        : column_reader_(pool)
        , pool_(pool)
    {
    }

    DataFrame read_buffer(
        std::size_t n, const std::uint8_t *buf, bool = false) override
    {
        ::bsoncxx::document::view doc(buf, n);
        ::dataframe::DataFrame data;

        for (auto &&col : doc) {
            auto b_key = col.key();
            std::string key(b_key.data(), b_key.size());
            data[key] = column_reader_.read(col.get_document().view());
        }

        return data;
    }

    ::arrow::MemoryPool *memory_pool() const { return pool_; }

  protected:
    bson::ColumnReader column_reader_;
    ::arrow::MemoryPool *pool_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_HPP
