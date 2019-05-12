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

TEST_CASE("Rcpp conversion", "[rcpp]")
{
    RInside R;

    using Date = ::dataframe::Datestamp<::dataframe::DateUnit::Day>;

    using Timestamp =
        ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>;

    ::dataframe::DataFrame df;

    df["UInt8"] = std::vector<std::uint8_t>({0, 1, 2, 3, 4, 5, 6, 7});
    df["Int8"] = ::dataframe::repeat(INT8_C(8));
    df["UInt16"] = ::dataframe::repeat(UINT16_C(16));
    df["Int16"] = ::dataframe::repeat(INT16_C(16));
    df["UInt32"] = ::dataframe::repeat(UINT32_C(32));
    df["Int32"] = ::dataframe::repeat(INT32_C(32));

    df["UInt64"] = ::dataframe::repeat(UINT64_C(64));
    df["Int64"] = ::dataframe::repeat(INT64_C(64));
    df["Float"] = ::dataframe::repeat(32.0f);
    df["Double"] = ::dataframe::repeat(64.0);

    df["Date"] = ::dataframe::repeat(Date(1234));
    df["Timestamp"] = ::dataframe::repeat(Timestamp(123456789));
    df["String"] = ::dataframe::repeat(std::string("string"));

    df["Bool"] = ::dataframe::repeat(true);

    // ::dataframe::CategoricalArray categorical;
    // for (std::size_t i = 0; i != df.nrow(); ++i) {
    //     categorical.push_back("categorical:" + std::to_string(i % 4));
    // }
    // df["Categorical"] = categorical;

    ::Rcpp::List list;
    ::dataframe::cast_dataframe(df, &list);

    auto ret = ::dataframe::make_dataframe(list);

    CHECK(ret["UInt8"].is_type<std::int32_t>());
    CHECK(ret["Int8"].is_type<std::int32_t>());
    CHECK(ret["UInt16"].is_type<std::int32_t>());
    CHECK(ret["Int16"].is_type<std::int32_t>());
    CHECK(ret["UInt32"].is_type<std::int32_t>());
    CHECK(ret["Int32"].is_type<std::int32_t>());

    CHECK(ret["UInt64"].is_type<double>());
    CHECK(ret["Int64"].is_type<double>());
    CHECK(ret["Float"].is_type<double>());
    CHECK(ret["Double"].is_type<double>());

    CHECK(ret["Date"].is_type<Date>());
    CHECK(ret["Timestamp"].is_type<Timestamp>());
    CHECK(ret["String"].is_type<std::string>());

    CHECK(ret["Bool"].is_type<bool>());

    // CHECK(ret["Categorical"].is_categorical());

    CHECK(ret["UInt8"].as<int>() == df["UInt8"].as<int>());
    CHECK(ret["Int8"].as<int>() == df["Int8"].as<int>());
    CHECK(ret["UInt16"].as<int>() == df["UInt16"].as<int>());
    CHECK(ret["Int16"].as<int>() == df["Int16"].as<int>());
    CHECK(ret["UInt32"].as<int>() == df["UInt32"].as<int>());
    CHECK(ret["Int32"].as<int>() == df["Int32"].as<int>());

    CHECK(ret["UInt64"].as<double>() == df["UInt64"].as<double>());
    CHECK(ret["Int64"].as<double>() == df["Int64"].as<double>());
    CHECK(ret["Float"].as<double>() == df["Float"].as<double>());
    CHECK(ret["Double"].as<double>() == df["Double"].as<double>());

    CHECK(ret["Date"].as<Date>() == df["Date"].as<Date>());
    CHECK(ret["Timestamp"].as<Timestamp>() == df["Timestamp"].as<Timestamp>());
    CHECK(ret["String"].as<std::string>() == df["String"].as<std::string>());

    CHECK(ret["Bool"].as<bool>() == df["Bool"].as<bool>());

    // CHECK(ret["Categorical"].as<::dataframe::Dict<std::string>>() ==
    //     df["Categorical"].as<::dataframe::Dict<std::string>>());
}
