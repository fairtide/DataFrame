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

#include <dataframe/array/cast.hpp>

#include <catch2/catch.hpp>

#define DEFINE_TEST_SECTION(T, U)                                             \
    SECTION(#U)                                                               \
    {                                                                         \
        auto builder = ::dataframe::make_builder<T>();                        \
        std::shared_ptr<::arrow::Array> array;                                \
        DF_ARROW_ERROR_HANDLER(builder->Finish(&array));                      \
        auto other = ::dataframe::cast_array<U>(array);                       \
        CHECK(::dataframe::is_type<U>(other));                                \
    }

#define DEFINE_TEST_CASE(T)                                                   \
    TEST_CASE("Cast for " #T, "[cast_array]")                                 \
    {                                                                         \
        DEFINE_TEST_SECTION(T, std::int8_t)                                   \
        DEFINE_TEST_SECTION(T, std::int16_t)                                  \
        DEFINE_TEST_SECTION(T, std::int32_t)                                  \
        DEFINE_TEST_SECTION(T, std::int64_t)                                  \
        DEFINE_TEST_SECTION(T, std::uint8_t)                                  \
        DEFINE_TEST_SECTION(T, std::uint16_t)                                 \
        DEFINE_TEST_SECTION(T, std::uint32_t)                                 \
        DEFINE_TEST_SECTION(T, std::uint64_t)                                 \
        DEFINE_TEST_SECTION(T, float)                                         \
        DEFINE_TEST_SECTION(T, double)                                        \
    }
DEFINE_TEST_CASE(std::int8_t)
DEFINE_TEST_CASE(std::int16_t)
DEFINE_TEST_CASE(std::int32_t)
DEFINE_TEST_CASE(std::int64_t)
DEFINE_TEST_CASE(std::uint8_t)
DEFINE_TEST_CASE(std::uint16_t)
DEFINE_TEST_CASE(std::uint32_t)
DEFINE_TEST_CASE(std::uint64_t)
DEFINE_TEST_CASE(float)
DEFINE_TEST_CASE(double)
#undef DEFINE_TEST_CASE

#define DEFINE_TEST_CASE(T)                                                   \
    TEST_CASE("Cast for " #T, "[cast_array]")                                 \
    {                                                                         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Second>)         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>)    \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>)    \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>)     \
    }
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Second>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>)
#undef DEFINE_TEST_CASE

#define DEFINE_TEST_CASE(T)                                                   \
    TEST_CASE("Cast for " #T, "[cast_array]")                                 \
    {                                                                         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time32<::dataframe::TimeUnit::Second>)            \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time32<::dataframe::TimeUnit::Millisecond>)       \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time32<::dataframe::TimeUnit::Microsecond>)       \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time32<::dataframe::TimeUnit::Nanosecond>)        \
    }
DEFINE_TEST_CASE(::dataframe::Time32<::dataframe::TimeUnit::Second>)
DEFINE_TEST_CASE(::dataframe::Time32<::dataframe::TimeUnit::Millisecond>)
DEFINE_TEST_CASE(::dataframe::Time32<::dataframe::TimeUnit::Microsecond>)
DEFINE_TEST_CASE(::dataframe::Time32<::dataframe::TimeUnit::Nanosecond>)
#undef DEFINE_TEST_CASE

#define DEFINE_TEST_CASE(T)                                                   \
    TEST_CASE("Cast check for " #T, "[cast_array]")                           \
    {                                                                         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time64<::dataframe::TimeUnit::Second>)            \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time64<::dataframe::TimeUnit::Millisecond>)       \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time64<::dataframe::TimeUnit::Microsecond>)       \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time64<::dataframe::TimeUnit::Nanosecond>)        \
    }
DEFINE_TEST_CASE(::dataframe::Time64<::dataframe::TimeUnit::Second>)
DEFINE_TEST_CASE(::dataframe::Time64<::dataframe::TimeUnit::Millisecond>)
DEFINE_TEST_CASE(::dataframe::Time64<::dataframe::TimeUnit::Microsecond>)
DEFINE_TEST_CASE(::dataframe::Time64<::dataframe::TimeUnit::Nanosecond>)
#undef DEFINE_TEST_CASE

TEST_CASE("Cast nested", "[cast_array]")
{
    DEFINE_TEST_SECTION(::dataframe::List<int>, ::dataframe::List<double>)

    DEFINE_TEST_SECTION(::dataframe::Struct<int>, ::dataframe::Struct<double>)

    DEFINE_TEST_SECTION(::dataframe::List<::dataframe::Struct<int>>,
        ::dataframe::List<::dataframe::Struct<double>>)

    DEFINE_TEST_SECTION(::dataframe::Struct<::dataframe::List<int>>,
        ::dataframe::Struct<::dataframe::List<double>>)
}
