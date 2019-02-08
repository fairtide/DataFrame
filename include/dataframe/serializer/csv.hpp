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

#ifndef DATAFRAME_SERIALIZER_CSV_HPP
#define DATAFRAME_SERIALIZER_CSV_HPP

#include <dataframe/serializer/base.hpp>

namespace dataframe {

class CSVWriter : public Writer
{
  public:
    explicit CSVWriter(char delim = ',')
        : delim_(delim)
    {
    }

    std::size_t size() const final { return buffer_.size(); }

    const std::uint8_t *data() const final
    {
        return reinterpret_cast<const std::uint8_t *>(buffer_.data());
    }

    void write(const DataFrame &df) final
    {
        auto nrow = df.nrow();
        auto ncol = df.ncol();

        std::vector<std::string> keys;
        for (std::size_t i = 0; i != ncol; ++i) {
            keys.push_back(df[i].name());
        }

        std::vector<std::vector<std::string>> rows(nrow);

        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];

            auto add_bool = [&](auto &&data) {
                auto ptr = data.begin();
                std::string t("true");
                std::string f("false");
                for (auto &&doc : rows) {
                    if (*ptr++) {
                        doc.push_back(t);
                    } else {
                        doc.push_back(f);
                    }
                }
            };

            auto add_number = [&](auto &&data) {
                auto ptr = data.data();
                for (auto &&doc : rows) {
                    doc.push_back(std::to_string(*ptr++));
                }
            };

            auto add_string = [&](auto &&data) {
                auto ptr = data.data();
                for (auto &&doc : rows) {
                    doc.emplace_back(std::move(*ptr++));
                }
            };

            switch (col.dtype()) {
                case DataType::Bool:
                    add_bool(col.as_view<bool>());
                    break;
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
                    add_string(col.as_view<std::string_view>());
                    break;
                case DataType::Date: {
                    auto data = col.as_view<Date>();
                    std::vector<std::string> strs;
                    strs.reserve(data.size());
                    for (auto &&v : data) {
                        strs.push_back(
                            ::boost::gregorian::to_iso_extended_string(v));
                    }
                    add_string(strs);
                } break;
                case DataType::Timestamp: {
                    auto data = col.as_view<Timestamp>();
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
                    add_string(col.as_view<std::string_view>());
                    break;
                case DataType::Unknown:
                    throw DataFrameException(
                        "Unknown dtype cannot be converted to JSON");
            }
        }

        std::stringstream ss;

        for (std::size_t i = 0; i != keys.size(); ++i) {
            if (i > 0) {
                ss << delim_;
            }
            ss << keys[i];
        }
        ss << '\n';

        for (auto &&r : rows) {
            for (std::size_t i = 0; i != r.size(); ++i) {
                if (i > 0) {
                    ss << delim_;
                }
                ss << r[i];
            }
            ss << '\n';
        }

        buffer_ = ss.str();
    }

  private:
    char delim_;
    std::string buffer_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_JSON_HPP
