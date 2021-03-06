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

#ifndef DATAFRAME_SERIALIZER_BASE_HPP
#define DATAFRAME_SERIALIZER_BASE_HPP

#include <dataframe/table.hpp>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <fstream>
#include <iostream>

namespace dataframe {

class CopyBufferReader : public ::arrow::io::BufferReader
{
  public:
    explicit CopyBufferReader(
        const std::uint8_t *bytes, std::int64_t len, ::arrow::MemoryPool *pool)
        : ::arrow::io::BufferReader(bytes, len)
        , pool_(pool)
    {
    }

    ::arrow::Status Read(
        int64_t nbytes, std::shared_ptr<::arrow::Buffer> *out) override
    {
        if (nbytes == 0) {
            *out = std::make_shared<::arrow::Buffer>(nullptr, 0);
            return ::arrow::Status::OK();
        }

        std::shared_ptr<::arrow::Buffer> ret;
        ARROW_RETURN_NOT_OK(::arrow::io::BufferReader::Read(nbytes, &ret));

        return ret->Copy(0, ret->size(), pool_, out);
    }

    ::arrow::Status ReadAt(std::int64_t position, std::int64_t nbytes,
        std::shared_ptr<::arrow::Buffer> *out) override
    {
        if (nbytes == 0) {
            *out = std::make_shared<::arrow::Buffer>(nullptr, 0);
            return ::arrow::Status::OK();
        }

        std::shared_ptr<::arrow::Buffer> ret;
        ARROW_RETURN_NOT_OK(
            ::arrow::io::BufferReader::ReadAt(position, nbytes, &ret));

        return ret->Copy(0, ret->size(), pool_, out);
    }

    bool supports_zero_copy() const override { return false; }

  private:
    ::arrow::MemoryPool *pool_;
};

class Writer
{
  public:
    Writer() = default;

    Writer(const Writer &) = delete;

    Writer(Writer &&) = delete;

    Writer &operator=(const Writer &) = delete;

    Writer &operator=(Writer &&) = delete;

    virtual ~Writer() = default;

    virtual std::size_t size() const = 0;

    virtual const std::uint8_t *data() const = 0;

    virtual void write(const DataFrame &df) = 0;

    virtual void write_file(const std::string &path, const DataFrame &df)
    {
        write(df);
        std::ofstream out(path, std::ios::out | std::ios::binary);
        out.write(reinterpret_cast<const char *>(data()),
            static_cast<std::streamsize>(size()));
    }

    virtual std::string str() const
    {
        if (data() == nullptr) {
            return std::string();
        }

        return std::string(reinterpret_cast<const char *>(data()), size());
    }
};

class Reader
{
  public:
    Reader() = default;

    Reader(const Reader &) = delete;

    Reader(Reader &&) = delete;

    Reader &operator=(const Reader &) = delete;

    Reader &operator=(Reader &&) = delete;

    virtual ~Reader() = default;

    virtual DataFrame read_buffer(
        std::size_t n, const std::uint8_t *buf, bool zero_copy) = 0;

    virtual DataFrame read_file(const std::string &path)
    {
        std::ifstream in(path, std::ios::in | std::ios::binary);

        return read(std::string(std::istreambuf_iterator<char>(in),
            std::istreambuf_iterator<char>()));
    }

    DataFrame read(const std::string &str, bool zero_copy = false)
    {
        return read_buffer(str.size(),
            reinterpret_cast<const uint8_t *>(str.data()), zero_copy);
    }

    template <typename Alloc>
    DataFrame read(
        const std::vector<std::uint8_t, Alloc> &buf, bool zero_copy = false)
    {
        return read_buffer(buf.size(), buf.data(), zero_copy);
    }

    DataFrame read(
        std::size_t n, const std::uint8_t *buf, bool zero_copy = false)
    {
        return read_buffer(n, buf, zero_copy);
    }
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BASE_HPP
