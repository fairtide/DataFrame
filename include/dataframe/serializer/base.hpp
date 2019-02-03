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

#include <dataframe/dataframe.hpp>
#include <string>

namespace dataframe {

template <typename Derived>
class Writer
{
  public:
    Writer() = default;

    Writer(const Writer &) = delete;

    Writer(Writer &&) = delete;

    Writer &operator=(const Writer &) = delete;

    Writer &operator=(Writer &&) = delete;

    std::string str() const
    {
        auto self = static_cast<const Derived *>(this);

        if (self->data() == nullptr) {
            return std::string();
        }

        return std::string(
            reinterpret_cast<const char *>(self->data()), self->size());
    }

    std::vector<std::uint8_t> buffer()
    {
        auto self = static_cast<const Derived *>(this);

        if (self->data() == nullptr) {
            return std::vector<std::uint8_t>();
        }

        return std::vector<std::uint8_t>(
            self->data(), self->data() + self->size());
    }
};

template <typename Derived>
class Reader
{
  public:
    Reader() = default;

    Reader(const Reader &) = delete;

    Reader(Reader &&) = delete;

    Reader &operator=(const Reader &) = delete;

    Reader &operator=(Reader &&) = delete;

    DataFrame read(const std::string &str)
    {
        auto self = static_cast<const Derived *>(this);
        return self->read_buffer(
            str.size(), reinterpret_cast<const uint8_t *>(str.data()));
    }

    template <typename Alloc>
    DataFrame read(const std::vector<std::uint8_t, Alloc> &buf)
    {
        auto self = static_cast<const Derived *>(this);
        return self->read_buffer(buf.size(), buf.data());
    }

    DataFrame read(std::size_t n, const std::uint8_t *buf)
    {
        auto self = static_cast<const Derived *>(this);
        return self->read_buffer(n, buf);
    }
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BASE_HPP
