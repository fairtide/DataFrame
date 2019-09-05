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

#include <dataframe/array/make.hpp>
#include <dataframe/array/view.hpp>

#include "make_data.hpp"

#include <catch2/catch.hpp>

TEMPLATE_TEST_CASE("Make Primitive array", "[make_array][template]", void,
    bool, std::uint8_t, std::int8_t, std::uint16_t, std::int16_t,
    std::uint32_t, std::int32_t, std::uint64_t, std::int64_t, float, double,
    std::string, ::dataframe::Bytes, ::dataframe::Opaque<int>,
    ::dataframe::Dict<std::string>,
    ::dataframe::Datestamp<::dataframe::DateUnit::Day>,
    ::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Second>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Second>,
    ::dataframe::Time<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Nanosecond>,
    ::dataframe::List<double>, ::dataframe::Struct<double>,
    ::dataframe::List<::dataframe::Struct<double>>,
    ::dataframe::Struct<::dataframe::List<double>>)
{
    std::size_t n = 1000;

    auto values = make_data<TestType>(n);

    SECTION("Make array")
    {
        auto data = ::dataframe::make_array<TestType>(values);
        auto view = ::dataframe::make_view<TestType>(data);

        CHECK(::dataframe::is_type<TestType>(data->type()));
        CHECK(data->length() == static_cast<std::int64_t>(n));

        if constexpr (std::is_same_v<TestType, void>) {
            CHECK(data->null_count() == data->length());
        } else {
            CHECK(data->null_count() == 0);
        }

        CHECK(std::equal(
            values.begin(), values.end(), view.begin(), view.end()));
    }

    SECTION("Make nullable array")
    {
        auto valids = make_data<bool>(n);

        auto data = ::dataframe::make_array<TestType>(values, valids);
        auto view = ::dataframe::make_view<TestType>(data);

        CHECK(::dataframe::is_type<TestType>(data->type()));
        CHECK(data->length() == static_cast<std::int64_t>(n));

        if constexpr (std::is_same_v<TestType, void>) {
            CHECK(data->null_count() == data->length());
        } else {
            auto null_count = static_cast<std::int64_t>(n) -
                std::accumulate(valids.begin(), valids.end(), INT64_C(0));
            CHECK(data->null_count() == null_count);
        }

        bool pass = true;
        for (std::size_t i = 0; i != n; ++i) {
            pass = !valids[i] || values[i] == view[i];
        }
        CHECK(pass);
    }
}
