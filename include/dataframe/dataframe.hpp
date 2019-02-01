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

#ifndef DATAFRAME_DATAFRAME_HPP
#define DATAFRAME_DATAFRAME_HPP

#include <dataframe/column.hpp>
#include <dataframe/io.hpp>

namespace dataframe {

/// \brief DataFrame in C++
class DataFrame
{
  public:
    DataFrame() = default;
    DataFrame(const DataFrame &) = delete;
    DataFrame(DataFrame &&) = default;
    DataFrame &operator=(const DataFrame &) = delete;
    DataFrame &operator=(DataFrame &&) = delete;

    DataFrame(std::shared_ptr<::arrow::Table> table)
        : table_(std::move(table))
    {
    }

    ConstColumnProxy operator[](const std::string &name) const
    {
        return ConstColumnProxy(name, table_);
    }

    ColumnProxy operator[](std::string name)
    {
        return ColumnProxy(std::move(name), table_);
    }

    template <DataFormat Format>
    void read(const ::arrow::Buffer &buffer)
    {
        table_ = ::dataframe::read<Format>(buffer);
    }

    template <DataFormat Format>
    void read(std::size_t n, const std::uint8_t *buffer)
    {
        table_ = ::dataframe::read<Format>(n, buffer);
    }

    template <DataFormat Format>
    std::shared_ptr<::arrow::Buffer> write() const
    {
        return ::dataframe::write<Format>(*table_);
    }

    void feather_read(const std::string &path)
    {
        table_ = ::dataframe::feather_read(path);
    }

    std::size_t feather_write(const std::string &path) const
    {
        return ::dataframe::feather_write(path, *table_);
    }

    const std::shared_ptr<::arrow::Table> &table() const { return table_; }

  private:
    std::shared_ptr<::arrow::Table> table_;
};

} // namespace dataframe

#endif // DATAFRAME_DATAFRAME_HPP
