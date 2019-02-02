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
#include <gtest/gtest.h>
#include <random>

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

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
