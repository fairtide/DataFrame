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

#include "make_data.hpp"

#include <catch2/catch.hpp>

#define BASIC_TEST_TYPES                                                      \
    bool, std::int8_t, std::int16_t, std::int32_t, std::int64_t,              \
        std::uint8_t, std::uint16_t, std::uint32_t, std::uint64_t, float,     \
        double, std::string, ::dataframe::Bytes,                              \
        ::dataframe::Dict<std::string>,                                       \
        ::dataframe::Datestamp<::dataframe::DateUnit::Day>,                   \
        ::dataframe::Timestamp<::dataframe::TimeUnit::Second>,                \
        ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>,           \
        ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>,           \
        ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>,            \
        ::dataframe::Time<::dataframe::TimeUnit::Second>,                     \
        ::dataframe::Time<::dataframe::TimeUnit::Millisecond>

#define TEST_TYPES                                                            \
    BASIC_TEST_TYPES, void, ::dataframe::Opaque<int>,                            \
        ::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>,           \
        ::dataframe::Time<::dataframe::TimeUnit::Microsecond>,                \
        ::dataframe::Time<::dataframe::TimeUnit::Nanosecond>,                 \
        ::dataframe::List<double>, ::dataframe::Struct<double>,               \
        ::dataframe::List<::dataframe::Struct<double>>,                       \
        ::dataframe::Struct<::dataframe::List<double>>,                       \
        (::dataframe::NamedStruct<TestStruct, double>),                       \
        (::dataframe::List<::dataframe::NamedStruct<TestStruct, double>>),    \
        (::dataframe::NamedStruct<TestStruct, ::dataframe::List<double>>)

struct TestStruct;

inline auto field_name(const TestStruct *, ::dataframe::FieldIndex<0>)
{
    return "Test";
}

template <typename T, typename Reader, typename Writer>
inline void TestSerializer(::dataframe::DataFrame &out)
{
    Reader reader;
    Writer writer;

    std::size_t n = 1000;
    ::dataframe::DataFrame dat;

    auto mask = make_data<bool>(n);
    dat["data"].emplace<T>(make_data<T>(n));
    dat["null"].emplace<T>(make_data<T>(n, mask.begin()), mask);

    CHECK(::arrow::ValidateArray(*dat["data"].data()).ok());
    CHECK(::arrow::ValidateArray(*dat["null"].data()).ok());

    auto outname = dat["data"].data()->type()->ToString();
    out[outname] = dat["data"].data();
    out[outname + " (null)"] = dat["null"].data();

    writer.write(dat);
    auto str = writer.str();
    auto ret = reader.read(str);

    auto data1 = dat["data"].data();
    auto null1 = dat["null"].data();

    auto data2 = ret["data"].data();
    auto null2 = ret["null"].data();

    CHECK(data1->length() == data2->length());
    CHECK(null1->length() == null2->length());

    CHECK(data1->null_count() == data2->null_count());
    CHECK(null1->null_count() == null2->null_count());

    CHECK(data1->Equals(data2));
    CHECK(null1->Equals(null2));

    for (auto &&chk : ::dataframe::split_rows(dat, n / 3)) {
        writer.write(chk);
        str = writer.str();
        ret = reader.read(str);

        data1 = chk["data"].data();
        null1 = chk["null"].data();

        data2 = ret["data"].data();
        null2 = ret["null"].data();

        CHECK(data1->length() == data2->length());
        CHECK(null1->length() == null2->length());

        CHECK(data1->null_count() == data2->null_count());
        CHECK(null1->null_count() == null2->null_count());

        CHECK(data1->Equals(data2));
        CHECK(null1->Equals(null2));
    }
}
