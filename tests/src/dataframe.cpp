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

#define BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG

#include <dataframe/dataframe.hpp>
#include <gtest/gtest.h>
#include <random>

inline std::vector<bool> generate_bool(std::size_t n)
{
    std::vector<bool> ret(n);
    std::mt19937_64 rng;
    std::bernoulli_distribution runif;
    for (std::size_t i = 0; i != n; ++i) {
        ret[i] = runif(rng);
    }

    return ret;
}

template <typename T>
inline std::vector<T> generate_int(std::size_t n)
{
    std::vector<T> ret(n);
    std::mt19937_64 rng;
    std::uniform_int_distribution<T> runif(
        std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
    for (auto &v : ret) {
        v = runif(rng);
    }

    return ret;
}

template <typename T>
inline std::vector<T> generate_real(std::size_t n)
{
    std::vector<T> ret(n);
    std::mt19937_64 rng;
    std::uniform_real_distribution<T> runif(0, 1);
    for (auto &v : ret) {
        v = runif(rng);
    }

    return ret;
}

inline std::vector<::dataframe::Date> generate_date(std::size_t n)
{
    std::vector<::dataframe::Date> ret(n);
    ::dataframe::Date epoch(1970, 1, 1);
    std::mt19937_64 rng;
    std::uniform_real_distribution<int> runif(0, 1000);
    for (auto &v : ret) {
        v = epoch + ::boost::gregorian::days(runif(rng));
    }

    return ret;
}

inline std::vector<::dataframe::Timestamp> generate_timestamp(std::size_t n)
{
    std::vector<::dataframe::Timestamp> ret(n);
    ::dataframe::Timestamp epoch(::dataframe::Date(1970, 1, 1));
    std::mt19937_64 rng;
    std::uniform_real_distribution<int> runif(0, 1000);
    for (auto &v : ret) {
        v = epoch + ::boost::posix_time::seconds(runif(rng));
    }

    return ret;
}

template <typename U, typename T>
inline std::vector<::dataframe::Timestamp> int2timestamp(
    const std::vector<T> &x)
{
    std::vector<::dataframe::Timestamp> ret(x.size());
    ::dataframe::Timestamp epoch(::dataframe::Date(1970, 1, 1));
    for (std::size_t i = 0; i != x.size(); ++i) {
        ret[i] = epoch + U(x[i]);
    }

    return ret;
}

TEST(DataFrame, Bool)
{
    auto x = generate_bool(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_FALSE(df["x"].is_ctype<bool>());
    EXPECT_TRUE(df["x"].is_convertible<bool>());
    EXPECT_EQ(df["x"].as<bool>(), x);
}

TEST(DataFrame, UInt8)
{
    auto x = generate_int<std::uint8_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::uint8_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::uint8_t>());
    EXPECT_EQ(df["x"].as<std::uint8_t>(), x);
    EXPECT_EQ(
        df["x"].view<std::uint8_t>(), ::dataframe::ArrayView<std::uint8_t>(x));
    EXPECT_EQ(df["x"].view<std::uint8_t>().data(),
        df["x"].as_view<std::uint8_t>().data());
}

TEST(DataFrame, Int8)
{
    auto x = generate_int<std::int8_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::int8_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int8_t>());
    EXPECT_EQ(df["x"].as<std::int8_t>(), x);
    EXPECT_EQ(
        df["x"].view<std::int8_t>(), ::dataframe::ArrayView<std::int8_t>(x));
    EXPECT_EQ(df["x"].view<std::int8_t>().data(),
        df["x"].as_view<std::int8_t>().data());
}

TEST(DataFrame, UInt16)
{
    auto x = generate_int<std::uint16_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::uint16_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::uint16_t>());
    EXPECT_EQ(df["x"].as<std::uint16_t>(), x);
    EXPECT_EQ(df["x"].view<std::uint16_t>(),
        ::dataframe::ArrayView<std::uint16_t>(x));
    EXPECT_EQ(df["x"].view<std::uint16_t>().data(),
        df["x"].as_view<std::uint16_t>().data());
}

TEST(DataFrame, Int16)
{
    auto x = generate_int<std::int16_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::int16_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int16_t>());
    EXPECT_EQ(df["x"].as<std::int16_t>(), x);
    EXPECT_EQ(
        df["x"].view<std::int16_t>(), ::dataframe::ArrayView<std::int16_t>(x));
    EXPECT_EQ(df["x"].view<std::int16_t>().data(),
        df["x"].as_view<std::int16_t>().data());
}

TEST(DataFrame, UInt32)
{
    auto x = generate_int<std::uint32_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::uint32_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::uint32_t>());
    EXPECT_EQ(df["x"].as<std::uint32_t>(), x);
    EXPECT_EQ(df["x"].view<std::uint32_t>(),
        ::dataframe::ArrayView<std::uint32_t>(x));
    EXPECT_EQ(df["x"].view<std::uint32_t>().data(),
        df["x"].as_view<std::uint32_t>().data());
}

TEST(DataFrame, Int32)
{
    auto x = generate_int<std::int32_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::int32_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int32_t>());
    EXPECT_EQ(df["x"].as<std::int32_t>(), x);
    EXPECT_EQ(
        df["x"].view<std::int32_t>(), ::dataframe::ArrayView<std::int32_t>(x));
    EXPECT_EQ(df["x"].view<std::int32_t>().data(),
        df["x"].as_view<std::int32_t>().data());
}

TEST(DataFrame, UInt64)
{
    auto x = generate_int<std::uint64_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::uint64_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::uint64_t>());
    EXPECT_EQ(df["x"].as<std::uint64_t>(), x);
    EXPECT_EQ(df["x"].view<std::uint64_t>(),
        ::dataframe::ArrayView<std::uint64_t>(x));
    EXPECT_EQ(df["x"].view<std::uint64_t>().data(),
        df["x"].as_view<std::uint64_t>().data());
}

TEST(DataFrame, Int64)
{
    auto x = generate_int<std::int64_t>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::int64_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int64_t>());
    EXPECT_EQ(df["x"].as<std::int64_t>(), x);
    EXPECT_EQ(
        df["x"].view<std::int64_t>(), ::dataframe::ArrayView<std::int64_t>(x));
    EXPECT_EQ(df["x"].view<std::int64_t>().data(),
        df["x"].as_view<std::int64_t>().data());
}

TEST(DataFrame, Float)
{
    auto x = generate_real<float>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<float>());
    EXPECT_TRUE(df["x"].is_convertible<float>());
    EXPECT_EQ(df["x"].as<float>(), x);
    EXPECT_EQ(df["x"].view<float>(), ::dataframe::ArrayView<float>(x));
    EXPECT_EQ(df["x"].view<float>().data(), df["x"].as_view<float>().data());
}

TEST(DataFrame, Double)
{
    auto x = generate_real<double>(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<double>());
    EXPECT_TRUE(df["x"].is_convertible<double>());
    EXPECT_EQ(df["x"].as<double>(), x);
    EXPECT_EQ(df["x"].view<double>(), ::dataframe::ArrayView<double>(x));
    EXPECT_EQ(df["x"].view<double>().data(), df["x"].as_view<double>().data());
}

TEST(DataFrame, Date)
{
    auto x = generate_date(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::int32_t>());
    EXPECT_TRUE(df["x"].is_convertible<::dataframe::Date>());
    EXPECT_EQ(df["x"].as<::dataframe::Date>(), x);
}

TEST(DataFrame, DateD)
{
    auto x = generate_int<std::int32_t>(1000);
    ::dataframe::DataFrame df;
    df["x"].emplace(x, ::dataframe::TimeUnit::Day);
    EXPECT_TRUE(df["x"].is_ctype<std::int32_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int64_t>());
    EXPECT_EQ(df["x"].as<std::int32_t>(), x);
}

TEST(DataFrame, Timestamp)
{
    auto x = generate_timestamp(1000);
    ::dataframe::DataFrame df;
    df["x"] = x;
    EXPECT_TRUE(df["x"].is_ctype<std::int64_t>());
    EXPECT_TRUE(df["x"].is_convertible<::dataframe::Timestamp>());
    EXPECT_EQ(df["x"].as<::dataframe::Timestamp>(), x);
}

TEST(DataFrame, TimestampS)
{
    auto x = generate_int<std::int64_t>(1000);
    ::dataframe::DataFrame df;
    df["x"].emplace(x, ::dataframe::TimeUnit::Second);
    EXPECT_TRUE(df["x"].is_ctype<std::int64_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int64_t>());
    EXPECT_EQ(df["x"].as<std::int64_t>(), x);
    auto t = int2timestamp<::boost::posix_time::seconds>(x);
    EXPECT_EQ(df["x"].as<::dataframe::Timestamp>(), t);
}

TEST(DataFrame, TimestampMS)
{
    auto x = generate_int<std::int64_t>(1000);
    ::dataframe::DataFrame df;
    df["x"].emplace(x, ::dataframe::TimeUnit::Millisecond);
    EXPECT_TRUE(df["x"].is_ctype<std::int64_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int64_t>());
    EXPECT_EQ(df["x"].as<std::int64_t>(), x);
    auto t = int2timestamp<::boost::posix_time::milliseconds>(x);
    EXPECT_EQ(df["x"].as<::dataframe::Timestamp>(), t);
}

TEST(DataFrame, TimestampUS)
{
    auto x = generate_int<std::int64_t>(1000);
    ::dataframe::DataFrame df;
    df["x"].emplace(x, ::dataframe::TimeUnit::Microsecond);
    EXPECT_TRUE(df["x"].is_ctype<std::int64_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int64_t>());
    EXPECT_EQ(df["x"].as<std::int64_t>(), x);
    auto t = int2timestamp<::boost::posix_time::microseconds>(x);
    EXPECT_EQ(df["x"].as<::dataframe::Timestamp>(), t);
}

TEST(DataFrame, TimestampNS)
{
    auto x = generate_int<std::int64_t>(1000);
    ::dataframe::DataFrame df;
    df["x"].emplace(x, ::dataframe::TimeUnit::Nanosecond);
    EXPECT_TRUE(df["x"].is_ctype<std::int64_t>());
    EXPECT_TRUE(df["x"].is_convertible<std::int64_t>());
    EXPECT_EQ(df["x"].as<std::int64_t>(), x);
    auto t = int2timestamp<::boost::posix_time::nanoseconds>(x);
    EXPECT_EQ(df["x"].as<::dataframe::Timestamp>(), t);
}

TEST(DataFrame, make_dataframe)
{
    struct C {
        int x = 1;
        double y = 2;
        std::string z() const { return "foo"; }
    };

    std::vector<C> c(1);

    auto df = ::dataframe::make_dataframe(c,                //
        std::pair{"x", &C::x},                              //
        std::pair{"y", &C::y},                              //
        std::pair{"z", &C::z},                              //
        std::pair{"xy", [](auto &&v) { return v.x + v.y; }} //
    );

    ::dataframe::DataFrame ret;
    ret["x"] = 1;
    ret["y"] = 2.0;
    ret["z"] = "foo";
    ret["xy"] = 3.0;

    EXPECT_EQ(df, ret);
}
