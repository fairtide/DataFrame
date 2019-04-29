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

#include <dataframe/dataframe.hpp>
#include <dataframe/serializer/bson.hpp>
#include <gtest/gtest.h>
#include <random>

template <typename T>
inline std::vector<T> generate_int(std::size_t n)
{
    std::vector<T> ret(n);
    std::mt19937_64 rng;
    std::uniform_int_distribution<T> dist(0, 127);
    for (std::size_t i = 0; i != n; ++i) {
        ret[i] = dist(rng);
    }

    return ret;
}

template <typename T>
inline std::vector<T> generate_real(std::size_t n)
{
    std::vector<T> ret(n);
    std::mt19937_64 rng;
    std::uniform_real_distribution<T> dist;
    for (std::size_t i = 0; i != n; ++i) {
        ret[i] = dist(rng);
    }

    return ret;
}

inline std::shared_ptr<::arrow::Array> generate_null(std::size_t n)
{
    return std::make_shared<::arrow::NullArray>(static_cast<std::int64_t>(n));
}

inline std::shared_ptr<::arrow::Array> generate_float16(std::size_t n)
{
    auto values = generate_int<std::uint16_t>(n);
    ::arrow::HalfFloatBuilder builder(::arrow::default_memory_pool());
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

inline std::shared_ptr<::arrow::Array> generate_date32(std::size_t n)
{
    auto values = generate_int<std::int32_t>(n);
    ::arrow::Date32Builder builder(::arrow::default_memory_pool());
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

inline std::shared_ptr<::arrow::Array> generate_date64(std::size_t n)
{
    auto values = generate_int<std::int64_t>(n);
    ::arrow::Date64Builder builder(::arrow::default_memory_pool());
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

template <::arrow::TimeUnit::type Unit>
inline std::shared_ptr<::arrow::Array> generate_timestamp(std::size_t n)
{
    auto values = generate_int<std::int64_t>(n);
    ::arrow::TimestampBuilder builder(
        std::make_shared<::arrow::TimestampType>(Unit),
        ::arrow::default_memory_pool());
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

template <::arrow::TimeUnit::type Unit>
inline std::shared_ptr<::arrow::Array> generate_time32(std::size_t n)
{
    auto values = generate_int<std::int32_t>(n);
    ::arrow::Time32Builder builder(std::make_shared<::arrow::Time32Type>(Unit),
        ::arrow::default_memory_pool());
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

template <::arrow::TimeUnit::type Unit>
inline std::shared_ptr<::arrow::Array> generate_time64(std::size_t n)
{
    auto values = generate_int<std::int64_t>(n);
    ::arrow::Time64Builder builder(std::make_shared<::arrow::Time64Type>(Unit),
        ::arrow::default_memory_pool());
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

// template <::arrow::IntervalType::Unit Unit>
// inline std::shared_ptr<::arrow::Array> generate_interval(std::size_t n)
// {
//     auto values = generate_int<std::int64_t>(n);
//     ::arrow::IntervalBuilder builder(
//         std::make_shared<::arrow::IntervalType>(Unit),
//         ::arrow::default_memory_pool());
//     DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));
//     std::shared_ptr<::arrow::Array> ret;
//     DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
//     return ret;
// }

inline std::shared_ptr<::arrow::Array> generate_pod(std::size_t n)
{
    ::arrow::FixedSizeBinaryBuilder builder(
        std::make_shared<::arrow::FixedSizeBinaryType>(7),
        ::arrow::default_memory_pool());
    auto values = generate_int<char>(7 * n);
    auto p = values.data();
    for (std::size_t i = 0; i != n; ++i, p += 7) {
        DF_ARROW_ERROR_HANDLER(builder.Append(p));
    }
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

inline std::shared_ptr<::arrow::Array> generate_bytes(std::size_t n)
{
    ::arrow::BinaryBuilder builder(::arrow::default_memory_pool());
    auto values = generate_int<char>(10 * n);
    auto p = values.data();
    std::mt19937 rng;
    std::uniform_int_distribution<std::int32_t> size(0, 10);
    for (std::size_t i = 0; i != n; ++i) {
        auto k = size(rng);
        DF_ARROW_ERROR_HANDLER(builder.Append(p, k));
        p += k;
    }
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

inline std::shared_ptr<::arrow::Array> generate_utf8(std::size_t n)
{
    ::arrow::StringBuilder builder(::arrow::default_memory_pool());
    auto values = generate_int<char>(10 * n);
    auto p = values.data();
    std::mt19937 rng;
    std::uniform_int_distribution<std::int32_t> size(0, 10);
    for (std::size_t i = 0; i != n; ++i) {
        auto k = size(rng);
        DF_ARROW_ERROR_HANDLER(builder.Append(p, k));
        p += k;
    }
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));
    return ret;
}

template <typename Gen>
inline void DoTest(Gen &&gen)
{
    ::dataframe::DataFrame dat;
    std::size_t n = 10000;
    dat["test"] = gen(n);

    ::dataframe::BSONWriter writer;
    writer.write(dat);
    auto str = writer.str();
    ::dataframe::BSONReader reader;
    auto ret = reader.read(str);

    auto array1 = dat["test"].data();
    auto array2 = ret["test"].data();
    EXPECT_TRUE(array1->Equals(array2));
}

TEST(SerializerBSON, Null) { DoTest(generate_null); }
TEST(SerializerBSON, Int8) { DoTest(generate_int<std::int8_t>); }
TEST(SerializerBSON, Int16) { DoTest(generate_int<std::int16_t>); }
TEST(SerializerBSON, Int32) { DoTest(generate_int<std::int32_t>); }
TEST(SerializerBSON, Int64) { DoTest(generate_int<std::int64_t>); }
TEST(SerializerBSON, UInt8) { DoTest(generate_int<std::uint8_t>); }
TEST(SerializerBSON, UInt16) { DoTest(generate_int<std::uint16_t>); }
TEST(SerializerBSON, UInt32) { DoTest(generate_int<std::uint32_t>); }
TEST(SerializerBSON, UInt64) { DoTest(generate_int<std::uint64_t>); }
TEST(SerializerBSON, HalfFloat) { DoTest(generate_float16); }
TEST(SerializerBSON, Float) { DoTest(generate_real<float>); }
TEST(SerializerBSON, Double) { DoTest(generate_real<double>); }
TEST(SerializerBSON, Date32) { DoTest(generate_date32); }
TEST(SerializerBSON, Date64) { DoTest(generate_date64); }

TEST(SerializerBSON, Timestamp_SECOND)
{
    DoTest(generate_timestamp<::arrow::TimeUnit::SECOND>);
}

TEST(SerializerBSON, Timestamp_MILLI)
{
    DoTest(generate_timestamp<::arrow::TimeUnit::MILLI>);
}

TEST(SerializerBSON, Timestamp_MICRO)
{
    DoTest(generate_timestamp<::arrow::TimeUnit::MICRO>);
}

TEST(SerializerBSON, Timestamp_NANO)
{
    DoTest(generate_timestamp<::arrow::TimeUnit::NANO>);
}

TEST(SerializerBSON, Time32_SECOND)
{
    DoTest(generate_time32<::arrow::TimeUnit::SECOND>);
}

TEST(SerializerBSON, Time32_MILLI)
{
    DoTest(generate_time32<::arrow::TimeUnit::MILLI>);
}

TEST(SerializerBSON, Time32_MICRO)
{
    DoTest(generate_time32<::arrow::TimeUnit::MICRO>);
}

TEST(SerializerBSON, Time32_NANO)
{
    DoTest(generate_time32<::arrow::TimeUnit::NANO>);
}

TEST(SerializerBSON, Time64_SECOND)
{
    DoTest(generate_time64<::arrow::TimeUnit::SECOND>);
}

TEST(SerializerBSON, Time64_MILLI)
{
    DoTest(generate_time64<::arrow::TimeUnit::MILLI>);
}

TEST(SerializerBSON, Time64_MICRO)
{
    DoTest(generate_time64<::arrow::TimeUnit::MICRO>);
}

TEST(SerializerBSON, Time64_NANO)
{
    DoTest(generate_time64<::arrow::TimeUnit::NANO>);
}

TEST(SerializerBSON, POD) { DoTest(generate_pod); }

// TEST(SerializerBSON, Interval_YEAR_MONTH)
// {
//     DoTest(generate_interval<::arrow::IntervalType::Unit::YEAR_MONTH>);
// }

// TEST(SerializerBSON, Internval_DAY_TIME)
// {
//     DoTest(generate_interval<::arrow::IntervalType::Unit::DAY_TIME>);
// }
