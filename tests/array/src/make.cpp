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

#include <catch2/catch.hpp>

TEMPLATE_TEST_CASE("Make Primitive array", "[make_array][template]",
    std::uint8_t, std::int8_t, std::uint16_t, std::int16_t, std::uint32_t,
    std::int32_t, std::uint64_t, std::int64_t, float, double,
    ::dataframe::Datestamp<::dataframe::DateUnit::Day>,
    ::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Second>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Second>,
    ::dataframe::Time<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Nanosecond>)
{
    using T = TestType;

    std::size_t n = 1000;

    std::mt19937_64 rng;
    std::uniform_int_distribution<> rval;
    std::bernoulli_distribution rbit;
    std::vector<T> values;
    std::vector<bool> valids;

    for (std::size_t i = 0; i != n; ++i) {
        values.emplace_back(static_cast<::dataframe::CType<T>>(rval(rng)));
        valids.push_back(rbit(rng));
    }

    SECTION("Make array")
    {
        auto array_ptr = ::dataframe::make_array<T>(values);

        auto &array =
            dynamic_cast<const ::dataframe::ArrayType<T> &>(*array_ptr);

        CHECK(array.length() == static_cast<std::int64_t>(n));
        CHECK(array.null_count() == 0);

        CHECK(std::equal(values.begin(), values.end(), array.raw_values(),
            array.raw_values() + n, [](auto &&v1, auto &&v2) {
                return static_cast<::dataframe::CType<T>>(v1) == v2;
            }));
    }

    SECTION("Make nullable array")
    {
        auto array_ptr = ::dataframe::make_array<T>(
            values.begin(), values.end(), valids.begin());

        auto &array =
            dynamic_cast<const ::dataframe::ArrayType<T> &>(*array_ptr);

        CHECK(array.length() == static_cast<std::int64_t>(n));
        CHECK(array.null_count() ==
            static_cast<std::int64_t>(n) -
                std::accumulate(valids.begin(), valids.end(), INT64_C(0)));

        CHECK(std::equal(values.begin(), values.end(), array.raw_values(),
            array.raw_values() + n, [](auto &&v1, auto &&v2) {
                return static_cast<::dataframe::CType<T>>(v1) == v2;
            }));
    }
}

TEST_CASE("Make String array", "[make_array]")
{
    std::size_t n = 1000;

    std::mt19937_64 rng;
    std::uniform_int_distribution<char> rchar;
    std::uniform_int_distribution<std::size_t> rsize(0, 100);
    std::vector<char> buf;
    std::vector<std::string> values;

    auto setbuf = [&]() {
        buf.resize(rsize(rng));
        for (auto &c : buf) {
            c = rchar(rng);
        }
    };

    for (std::size_t i = 0; i != n; ++i) {
        setbuf();
        values.emplace_back(buf.data(), buf.size());
    }

    auto array_ptr = ::dataframe::make_array<std::string>(values);

    auto &array = dynamic_cast<const ::arrow::StringArray &>(*array_ptr);

    bool pass = true;
    for (std::size_t i = 0; i != n; ++i) {
        pass =
            pass && values[i] == array.GetView(static_cast<std::int64_t>(i));
    }

    CHECK(pass);
}

template <typename T>
struct TestEntry {
    T value;
    T get() const { return value; }
};

struct TestStruct {
    template <typename RNG, typename Dist>
    TestStruct(RNG &rng, Dist &dist)
    {
        float_entry.value = static_cast<double>(dist(rng));
        int_entry.value = static_cast<int>(dist(rng));
    }

    TestEntry<double> float_entry;
    TestEntry<int> int_entry;
};

DF_DEFINE_STRUCT_FIELD(TestStruct, 0, "float",
    [](const TestStruct &v) { return v.float_entry.get(); })

DF_DEFINE_STRUCT_FIELD(TestStruct, 1, "int",
    [](const TestStruct &v) { return v.int_entry.get(); })

TEST_CASE("Make Struct array", "[make_array]")
{
    std::size_t n = 1000;

    std::mt19937_64 rng;
    std::uniform_int_distribution<> rval;
    std::vector<TestStruct> values;
    for (std::size_t i = 0; i != n; ++i) {
        values.emplace_back(rng, rval);
    }

    auto array_ptr =
        ::dataframe::make_array<::dataframe::Struct<double, double>>(values);

    auto &array = dynamic_cast<const ::arrow::StructArray &>(*array_ptr);

    REQUIRE(array.num_fields() == 2);

    auto field1 = array.type()->child(0);
    CHECK(field1->name() == "float");

    auto field2 = array.type()->child(1);
    CHECK(field2->name() == "int");

    auto float_array_ptr = array.GetFieldByName("float");
    auto int_array_ptr = array.GetFieldByName("int");

    REQUIRE(float_array_ptr != nullptr);
    REQUIRE(int_array_ptr != nullptr);

    auto &float_array =
        dynamic_cast<const ::arrow::DoubleArray &>(*float_array_ptr);

    auto &int_array =
        dynamic_cast<const ::arrow::DoubleArray &>(*int_array_ptr);

    CHECK(std::equal(values.begin(), values.end(), float_array.raw_values(),
        float_array.raw_values() + n,
        [](auto &&v1, auto &&v2) { return v1.float_entry.value == v2; }));

    CHECK(std::equal(values.begin(), values.end(), int_array.raw_values(),
        int_array.raw_values() + n,
        [](auto &&v1, auto &&v2) { return v1.int_entry.value == v2; }));
}

TEST_CASE("Make List array", "[make_array]")
{
    std::size_t n = 1000;

    std::mt19937_64 rng;
    std::uniform_int_distribution<> rval;
    std::uniform_int_distribution<std::size_t> rsize(0, 100);
    std::vector<std::vector<int>> values(n);

    for (auto &v : values) {
        v.resize(rsize(rng));
        for (auto &u : v) {
            u = rval(rng);
        }
    }

    auto array_ptr =
        ::dataframe::make_array<::dataframe::List<std::int32_t>>(values);

    auto &array = dynamic_cast<const ::arrow::ListArray &>(*array_ptr);

    auto &array_values =
        dynamic_cast<const ::arrow::Int32Array &>(*array.values());

    bool pass = true;
    for (std::size_t i = 0; i != n; ++i) {
        auto &v = values[i];
        auto j = static_cast<std::int64_t>(i);
        auto begin = array_values.raw_values() + array.value_offset(j);
        auto end = begin + array.value_length(j);
        pass = std::equal(begin, end, v.begin(), v.end());
    }

    CHECK(pass);
}

TEST_CASE("Make List<Struct> array", "[make_array]")
{
    using T = std::pair<std::vector<TestStruct>, std::vector<TestStruct>>;

    std::size_t n = 1000;

    std::mt19937_64 rng;
    std::uniform_int_distribution<> rval;
    std::uniform_int_distribution<std::size_t> rsize(0, 100);
    std::vector<T> values;

    for (std::size_t i = 0; i != n; ++i) {
        auto m1 = rsize(rng);
        auto m2 = rsize(rng);
        std::vector<TestStruct> list1;
        std::vector<TestStruct> list2;
        for (std::size_t j = 0; j != m1; ++j) {
            list1.emplace_back(rng, rval);
        }
        for (std::size_t j = 0; j != m2; ++j) {
            list2.emplace_back(rng, rval);
        }
        values.emplace_back(std::move(list1), std::move(list2));
    }

    auto array_ptr = ::dataframe::make_array<
        ::dataframe::List<::dataframe::Struct<double, int>>>(
        ::dataframe::field_iterator(values.begin(), &T::first),
        ::dataframe::field_iterator(values.end(), &T::first));

    auto &array = dynamic_cast<const ::arrow::ListArray &>(*array_ptr);

    auto &array_values =
        dynamic_cast<const ::arrow::StructArray &>(*array.values());

    auto float_values_ptr = array_values.GetFieldByName("float");
    auto int_values_ptr = array_values.GetFieldByName("int");

    REQUIRE(float_values_ptr != nullptr);
    REQUIRE(int_values_ptr != nullptr);

    auto float_values =
        dynamic_cast<const ::arrow::DoubleArray &>(*float_values_ptr)
            .raw_values();

    auto int_values =
        dynamic_cast<const ::arrow::Int32Array &>(*int_values_ptr)
            .raw_values();

    bool pass = true;
    for (std::size_t i = 0; i != n; ++i) {
        auto &v = values[i].first;
        auto j = static_cast<std::int64_t>(i);
        auto first = array.value_offset(j);
        auto last = first + array.value_length(j);
        pass = pass &&
            std::equal(float_values + first, float_values + last, v.begin(),
                v.end(), [](auto &&v1, auto &&v2) {
                    return v1 == v2.float_entry.value;
                });
        pass = pass &&
            std::equal(int_values + first, int_values + last, v.begin(),
                v.end(),
                [](auto &&v1, auto &&v2) { return v1 == v2.int_entry.value; });
    }
    CHECK(pass);
}
