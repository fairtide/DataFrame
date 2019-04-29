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
    std::uniform_int_distribution<T> dist;
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

inline auto generate_float16(std::size_t n)
{
    auto values = generate_int<std::uint16_t>(n);

    ::arrow::HalfFloatBuilder builder;
    DF_ARROW_ERROR_HANDLER(builder.AppendValues(values));

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

template <typename Gen>
inline void DoTest(Gen &&gen)
{
    ::dataframe::DataFrame df;
    std::size_t n = 1000;
    df["test"] = gen(n);

    ::dataframe::BSONWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::BSONReader reader;
    auto ret = reader.read(str);

    EXPECT_TRUE(df["test"].data()->Equals(ret["test"].data()));
}

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
