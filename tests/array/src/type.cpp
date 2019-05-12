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

#include <dataframe/array/type.hpp>

#include <catch2/catch.hpp>

#define DEFINE_TEST_SECTION(T, U)                                             \
    SECTION(#U)                                                               \
    {                                                                         \
        auto data_type = ::dataframe::make_data_type<T>();                    \
        CHECK(::dataframe::is_type<T>(data_type));                            \
        if constexpr (!std::is_same_v<T, U>) {                                \
            CHECK(!::dataframe::is_type<U>(data_type));                       \
        }                                                                     \
    }

#define DEFINE_TEST_CASE(T)                                                   \
    TEST_CASE("Type check for " #T, "[is_type]")                              \
    {                                                                         \
        DEFINE_TEST_SECTION(T, void)                                          \
        DEFINE_TEST_SECTION(T, bool)                                          \
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
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Datestamp<::dataframe::DateUnit::Day>)            \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>)    \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Second>)         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>)    \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>)    \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>)     \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time<::dataframe::TimeUnit::Second>)              \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time<::dataframe::TimeUnit::Millisecond>)         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time<::dataframe::TimeUnit::Microsecond>)         \
        DEFINE_TEST_SECTION(                                                  \
            T, ::dataframe::Time<::dataframe::TimeUnit::Nanosecond>)          \
        DEFINE_TEST_SECTION(T, std::string)                                   \
        DEFINE_TEST_SECTION(T, ::dataframe::Bytes)                            \
        DEFINE_TEST_SECTION(T, ::dataframe::List<int>)                        \
        DEFINE_TEST_SECTION(T, ::dataframe::List<double>)                     \
        DEFINE_TEST_SECTION(T, ::dataframe::Struct<int>)                      \
        DEFINE_TEST_SECTION(T, ::dataframe::Struct<double>)                   \
        DEFINE_TEST_SECTION(T, ::dataframe::List<::dataframe::Struct<int>>)   \
        DEFINE_TEST_SECTION(T, ::dataframe::Struct<::dataframe::List<int>>)   \
    }

DEFINE_TEST_CASE(void)
DEFINE_TEST_CASE(bool)
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
DEFINE_TEST_CASE(::dataframe::Datestamp<::dataframe::DateUnit::Day>)
DEFINE_TEST_CASE(::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Second>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>)
DEFINE_TEST_CASE(::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>)
DEFINE_TEST_CASE(::dataframe::Time<::dataframe::TimeUnit::Second>)
DEFINE_TEST_CASE(::dataframe::Time<::dataframe::TimeUnit::Millisecond>)
DEFINE_TEST_CASE(::dataframe::Time<::dataframe::TimeUnit::Microsecond>)
DEFINE_TEST_CASE(::dataframe::Time<::dataframe::TimeUnit::Nanosecond>)
DEFINE_TEST_CASE(std::string)
DEFINE_TEST_CASE(::dataframe::Bytes)
DEFINE_TEST_CASE(::dataframe::List<int>)
DEFINE_TEST_CASE(::dataframe::List<double>)
DEFINE_TEST_CASE(::dataframe::Struct<int>)
DEFINE_TEST_CASE(::dataframe::Struct<double>)
DEFINE_TEST_CASE(::dataframe::List<::dataframe::Struct<int>>)
DEFINE_TEST_CASE(::dataframe::Struct<::dataframe::List<int>>)
