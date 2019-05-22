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

#include <dataframe/table/rcpp.hpp>

#include <RInside.h>

#include <catch2/catch.hpp>

#include "make_data.hpp"

TEST_CASE("Rcpp conversion", "[rcpp]")
{
    RInside R;

    ::dataframe::DataFrame df;

    std::size_t n = 10;

    using Date = ::dataframe::Datestamp<::dataframe::DateUnit::Day>;

    using Timestamp =
        ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>;

    using Factor = ::dataframe::Dict<std::string>;

    df["Bool"] = make_data<bool>(n);
    df["Int8"] = make_data<std::int8_t>(n);
    df["Int16"] = make_data<std::int16_t>(n);
    df["Int32"] = make_data<std::int32_t>(n);
    df["Int64"] = make_data<std::int64_t>(n);
    df["UInt8"] = make_data<std::uint8_t>(n);
    df["UInt16"] = make_data<std::uint16_t>(n);
    df["UInt32"] = make_data<std::uint32_t>(n);
    df["UInt64"] = make_data<std::uint64_t>(n);
    df["Float"] = make_data<float>(n);
    df["Double"] = make_data<double>(n);
    df["Date"] = make_data<Date>(n);
    df["Timestamp"] = make_data<Timestamp>(n);
    df["String"] = make_data<std::string>(n);
    df["Factor"].emplace<Factor>(make_data<Factor>(n));

    ::Rcpp::List list;
    ::dataframe::cast_dataframe(df, &list);

    auto ret = ::dataframe::make_dataframe(list);

    CHECK(ret["Bool"].is_type<bool>());
    CHECK(ret["Int8"].is_type<std::int32_t>());
    CHECK(ret["Int16"].is_type<std::int32_t>());
    CHECK(ret["Int32"].is_type<std::int32_t>());
    CHECK(ret["Int64"].is_type<double>());
    CHECK(ret["UInt8"].is_type<std::int32_t>());
    CHECK(ret["UInt16"].is_type<std::int32_t>());
    CHECK(ret["UInt32"].is_type<std::int32_t>());
    CHECK(ret["UInt64"].is_type<double>());
    CHECK(ret["Float"].is_type<double>());
    CHECK(ret["Double"].is_type<double>());
    CHECK(ret["Date"].is_type<Date>());
    CHECK(ret["Timestamp"].is_type<Timestamp>());
    CHECK(ret["String"].is_type<std::string>());
    CHECK(ret["Factor"].is_type<Factor>());

    CHECK(ret["Bool"].as<bool>() == df["Bool"].as<bool>());
    CHECK(ret["Int8"].as<int>() == df["Int8"].as<int>());
    CHECK(ret["Int16"].as<int>() == df["Int16"].as<int>());
    CHECK(ret["Int32"].as<int>() == df["Int32"].as<int>());
    CHECK(ret["Int64"].as<double>() == df["Int64"].as<double>());
    CHECK(ret["UInt8"].as<int>() == df["UInt8"].as<int>());
    CHECK(ret["UInt16"].as<int>() == df["UInt16"].as<int>());
    CHECK(ret["UInt32"].as<int>() == df["UInt32"].as<int>());
    CHECK(ret["UInt64"].as<double>() == df["UInt64"].as<double>());
    CHECK(ret["Float"].as<double>() == df["Float"].as<double>());
    CHECK(ret["Double"].as<double>() == df["Double"].as<double>());
    CHECK(ret["Date"].as<Date>() == df["Date"].as<Date>());
    CHECK(ret["Timestamp"].as<Timestamp>() == df["Timestamp"].as<Timestamp>());
    CHECK(ret["String"].as<std::string>() == df["String"].as<std::string>());
    CHECK(ret["Factor"].as<Factor>() == df["Factor"].as<Factor>());
}
