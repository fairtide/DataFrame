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

#ifndef DATAFRAME_SERIALIZER_JSON_HPP
#define DATAFRAME_SERIALIZER_JSON_HPP

#include <dataframe/serializer/base.hpp>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace dataframe {

class JSONRowWriter : public Writer
{
  public:
    JSONRowWriter(std::string root)
        : root_(std::move(root))
    {
        if (root_.empty()) {
            throw DataFrameException("root key cannot be empty");
        }
    }

    std::size_t size() const final { return buffer_.GetSize(); }

    const std::uint8_t *data() const final
    {
        return reinterpret_cast<const std::uint8_t *>(buffer_.GetString());
    }

    void write(const DataFrame &df) final
    {
        auto nrow = df.nrow();
        auto ncol = df.ncol();

        ::rapidjson::Document root;
        root.SetObject();

        std::vector<::rapidjson::Document> rows;
        for (std::size_t i = 0; i != nrow; ++i) {
            rows.emplace_back(::rapidjson::kObjectType, &root.GetAllocator());
        }

        std::vector<std::string> keys;
        for (std::size_t i = 0; i != ncol; ++i) {
            keys.push_back(df[i].name());
        }

        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];
            auto key = ::rapidjson::StringRef(keys[i].data(), keys[i].size());

            auto add_number = [&](auto &&data) {
                auto ptr = data.data();
                for (auto &&doc : rows) {
                    doc.AddMember(key, *ptr++, doc.GetAllocator());
                }
            };

            auto add_string = [&](auto &&data) {
                auto ptr = data.data();
                for (auto &&doc : rows) {
                    doc.AddMember(key,
                        ::rapidjson::Value().SetString(ptr->data(),
                            static_cast<unsigned>(ptr->size()),
                            doc.GetAllocator()),
                        doc.GetAllocator());
                    ++ptr;
                }
            };

            auto add_string_view = [&](auto &&data) {
                auto ptr = data.data();
                for (auto &&doc : rows) {
                    doc.AddMember(key,
                        ::rapidjson::StringRef(ptr->data(), ptr->size()),
                        doc.GetAllocator());
                    ++ptr;
                }
            };

            switch (col.dtype()) {
                case DataType::UInt8:
                    add_number(col.as_view<std::uint8_t>());
                    break;
                case DataType::Int8:
                    add_number(col.as_view<std::uint8_t>());
                    break;
                case DataType::UInt16:
                    add_number(col.as_view<std::int8_t>());
                    break;
                case DataType::Int16:
                    add_number(col.as_view<std::uint16_t>());
                    break;
                case DataType::UInt32:
                    add_number(col.as_view<std::uint32_t>());
                    break;
                case DataType::Int32:
                    add_number(col.as_view<std::int32_t>());
                    break;
                case DataType::UInt64:
                    add_number(col.as_view<std::uint64_t>());
                    break;
                case DataType::Int64:
                    add_number(col.as_view<std::int64_t>());
                    break;
                case DataType::Float:
                    add_number(col.as_view<float>());
                    break;
                case DataType::Double:
                    add_number(col.as_view<double>());
                    break;
                case DataType::String:
                    add_string_view(col.as<std::string_view>());
                    break;
                case DataType::Date: {
                    auto data = col.as<Date>();
                    std::vector<std::string> strs;
                    strs.reserve(data.size());
                    for (auto &&v : data) {
                        strs.push_back(
                            ::boost::gregorian::to_iso_extended_string(v));
                    }
                    add_string(strs);
                } break;
                case DataType::Timestamp: {
                    auto data = col.as<Timestamp>();
                    std::vector<std::string> strs;
                    strs.reserve(data.size());
                    for (auto &&v : data) {
                        strs.push_back(
                            ::boost::posix_time::to_iso_extended_string(v) +
                            "Z");
                    }
                    add_string(strs);
                } break;
                case DataType::Categorical:
                    add_string_view(col.as<std::string_view>());
                    break;
                case DataType::Unknown:
                    throw DataFrameException(
                        "Unknown dtype cannot be converted to JSON");
            }
        }

        ::rapidjson::Value data(::rapidjson::kArrayType);
        for (auto &r : rows) {
            data.PushBack(std::move(r), root.GetAllocator());
        }

        root.AddMember(::rapidjson::StringRef(root_.data(), root_.size()),
            data, root.GetAllocator());

        ::rapidjson::Writer<::rapidjson::StringBuffer> visitor(buffer_);
        root.Accept(visitor);
    }

  private:
    std::string root_;
    ::rapidjson::StringBuffer buffer_;
};

class JSONColumnWriter : public Writer
{
  public:
    JSONColumnWriter(std::string root)
        : root_(std::move(root))
    {
        if (root_.empty()) {
            throw DataFrameException("root key cannot be empty");
        }
    }

    std::size_t size() const final { return buffer_.GetSize(); }

    const std::uint8_t *data() const final
    {
        return reinterpret_cast<const std::uint8_t *>(buffer_.GetString());
    }

    void write(const DataFrame &df) final
    {
        auto ncol = df.ncol();

        ::rapidjson::Document root(::rapidjson::kObjectType);
        ::rapidjson::Document cols(
            ::rapidjson::kObjectType, &root.GetAllocator());

        std::vector<std::string> keys;
        for (std::size_t i = 0; i != ncol; ++i) {
            keys.push_back(df[i].name());
        }

        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];
            auto key = ::rapidjson::StringRef(keys[i].data(), keys[i].size());

            auto add_number = [&](auto &&data) {
                ::rapidjson::Value value(::rapidjson::kArrayType);
                for (auto &&v : data) {
                    value.PushBack(v, root.GetAllocator());
                }
                cols.AddMember(key, value, cols.GetAllocator());
            };

            auto add_string = [&](auto &&data) {
                ::rapidjson::Value value(::rapidjson::kArrayType);
                for (auto &&v : data) {
                    value.PushBack(::rapidjson::Value().SetString(v.data(),
                                       static_cast<unsigned>(v.size()),
                                       root.GetAllocator()),
                        root.GetAllocator());
                }
                cols.AddMember(key, value, cols.GetAllocator());
            };

            auto add_string_view = [&](auto &&data) {
                ::rapidjson::Value value(::rapidjson::kArrayType);
                for (auto &&v : data) {
                    value.PushBack(::rapidjson::StringRef(v.data(), v.size()),
                        root.GetAllocator());
                }
                cols.AddMember(key, value, cols.GetAllocator());
            };

            switch (col.dtype()) {
                case DataType::UInt8:
                    add_number(col.as_view<std::uint8_t>());
                    break;
                case DataType::Int8:
                    add_number(col.as_view<std::uint8_t>());
                    break;
                case DataType::UInt16:
                    add_number(col.as_view<std::int8_t>());
                    break;
                case DataType::Int16:
                    add_number(col.as_view<std::uint16_t>());
                    break;
                case DataType::UInt32:
                    add_number(col.as_view<std::uint32_t>());
                    break;
                case DataType::Int32:
                    add_number(col.as_view<std::int32_t>());
                    break;
                case DataType::UInt64:
                    add_number(col.as_view<std::uint64_t>());
                    break;
                case DataType::Int64:
                    add_number(col.as_view<std::int64_t>());
                    break;
                case DataType::Float:
                    add_number(col.as_view<float>());
                    break;
                case DataType::Double:
                    add_number(col.as_view<double>());
                    break;
                case DataType::String:
                    add_string_view(col.as<std::string_view>());
                    break;
                case DataType::Date: {
                    auto data = col.as<Date>();
                    std::vector<std::string> strs;
                    strs.reserve(data.size());
                    for (auto &&v : data) {
                        strs.push_back(
                            ::boost::gregorian::to_iso_extended_string(v));
                    }
                    add_string(strs);
                } break;
                case DataType::Timestamp: {
                    auto data = col.as<Timestamp>();
                    std::vector<std::string> strs;
                    strs.reserve(data.size());
                    for (auto &&v : data) {
                        strs.push_back(
                            ::boost::posix_time::to_iso_extended_string(v) +
                            "Z");
                    }
                    add_string(strs);
                } break;
                case DataType::Categorical:
                    add_string_view(col.as<std::string_view>());
                    break;
                case DataType::Unknown:
                    throw DataFrameException(
                        "Unknown dtype cannot be converted to JSON");
            }
        }

        root.AddMember(::rapidjson::StringRef(root_.data(), root_.size()),
            cols, root.GetAllocator());

        ::rapidjson::Writer<::rapidjson::StringBuffer> visitor(buffer_);
        root.Accept(visitor);
    }

  private:
    std::string root_;
    ::rapidjson::StringBuffer buffer_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_JSON_HPP
