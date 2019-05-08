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

#include <dataframe/array/view.hpp>

#include <catch2/catch.hpp>

namespace dataframe {

template <typename T1, typename T2>
inline bool operator==(const ListView<T1> &view, const std::vector<T2> &value)
{
    return std::equal(view.begin(), view.end(), value.begin(), value.end());
}

template <typename S, typename... Types>
inline bool operator==(const S &view, const std::tuple<Types...> &value)
{
    return view.get() == value;
}

template <typename T>
struct TestDataBase {
    using value_type = T;

    TestDataBase(std::size_t n, bool nullable)
        : length(static_cast<std::int64_t>(n))
    {
        if (nullable) {
            std::mt19937_64 rng;
            std::bernoulli_distribution rbit;

            DF_ARROW_ERROR_HANDLER(
                ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
                    ::arrow::BitUtil::BytesForBits(length), &null_bitmap));

            auto bits = dynamic_cast<::arrow::MutableBuffer &>(*null_bitmap)
                            .mutable_data();

            for (std::int64_t i = 0; i != length; ++i) {
                valids.push_back(rbit(rng));
                ::arrow::BitUtil::SetBitTo(bits, i, valids.back());
            }
        }
    }

    template <typename U>
    void check(const ArrayView<U> &view, std::int64_t offset = 0)
    {
        CHECK(static_cast<std::int64_t>(view.size()) == length - offset);

        std::int64_t nc = 0;
        if (!valids.empty()) {
            for (auto iter = valids.begin() + offset; iter != valids.end();
                 ++iter) {
                nc += !*iter;
            }
        }
        CHECK(view.data()->null_count() == nc);

        CHECK(std::equal(
            view.begin(), view.end(), values.begin() + offset, values.end()));
    }

    std::int64_t length;
    std::vector<bool> valids;
    std::vector<T> values;
    std::shared_ptr<::arrow::Buffer> null_bitmap;
    std::shared_ptr<::arrow::Array> array;
};

template <typename T>
struct TestData : TestDataBase<T> {
    TestData(std::size_t n = 0, bool nullable = false)
        : TestDataBase<T>(n, nullable)
    {
        using traits = TypeTraits<T>;
        using U = typename traits::ctype;

        std::mt19937_64 rng;
        std::uniform_int_distribution<> rval;
        std::vector<U> rawval;

        for (std::size_t i = 0; i != n; ++i) {
            rawval.push_back(static_cast<U>(rval(rng)));
            this->values.emplace_back(rawval.back());
        }

        auto builder = traits::builder();

        if (nullable) {
            DF_ARROW_ERROR_HANDLER(
                builder->AppendValues(rawval, this->valids));
        } else {
            DF_ARROW_ERROR_HANDLER(builder->AppendValues(rawval));
        }

        DF_ARROW_ERROR_HANDLER(builder->Finish(&this->array));
    }
};

template <typename T>
struct TestData<ListView<T>>
    : TestDataBase<std::vector<typename TestData<T>::value_type>> {
    TestData(std::size_t n = 0, bool nullable = false)
        : TestDataBase<std::vector<typename TestData<T>::value_type>>(
              n, nullable)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::int32_t> rval(0, 100);

        std::shared_ptr<::arrow::Buffer> offsets;

        DF_ARROW_ERROR_HANDLER(
            ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
                static_cast<std::int64_t>((n + 1) * sizeof(std::int32_t)),
                &offsets));

        auto p = reinterpret_cast<std::int32_t *>(
            dynamic_cast<::arrow::MutableBuffer &>(*offsets).mutable_data());

        p[0] = 0;
        for (std::size_t i = 1; i <= n; ++i) {
            p[i] = p[i - 1] + rval(rng);
        }

        data = TestData<T>(static_cast<std::size_t>(p[n]), nullable);

        for (std::int64_t i = 0; i != this->length; ++i) {
            this->values.emplace_back(
                data.values.data() + p[i], data.values.data() + p[i + 1]);
        }

        this->array = std::make_shared<::arrow::ListArray>(
            ::arrow::list(data.array->type()), this->length, offsets,
            data.array, this->null_bitmap, ::arrow::kUnknownNullCount);
    }

    TestData<T> data;
};

template <typename... Args>
struct TestData<StructView<Args...>>
    : TestDataBase<std::tuple<typename TestData<Args>::value_type...>> {
    static constexpr std::size_t nfields = sizeof...(Args);

    TestData(std::size_t n = 0, bool nullable = false)
        : TestDataBase<std::tuple<typename TestData<Args>::value_type...>>(
              n, nullable)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        std::vector<std::shared_ptr<::arrow::Array>> children;

        this->values.resize(n);
        set<0>(n, nullable, fields, children,
            std::integral_constant<bool, 0 < nfields>());

        this->array = std::make_shared<::arrow::StructArray>(
            ::arrow::struct_(fields), this->length, children,
            this->null_bitmap, ::arrow::kUnknownNullCount);
    }

    template <std::size_t N>
    void set(std::size_t n, bool nullable,
        std::vector<std::shared_ptr<::arrow::Field>> &fields,
        std::vector<std::shared_ptr<::arrow::Array>> &children, std::true_type)
    {
        std::get<N>(data) =
            TestData<std::tuple_element_t<N, std::tuple<Args...>>>(
                n, nullable);

        fields.push_back(
            ::arrow::field(std::get<N>(data).array->type()->name(),
                std::get<N>(data).array->type()));

        for (std::size_t i = 0; i != n; ++i) {
            std::get<N>(this->values[i]) = std::get<N>(data).values[i];
        }

        children.push_back(std::get<N>(data).array);

        set<N + 1>(n, nullable, fields, children,
            std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    void set(std::size_t, bool, std::vector<std::shared_ptr<::arrow::Field>> &,
        std::vector<std::shared_ptr<::arrow::Array>> &, std::false_type)
    {
    }

    std::tuple<TestData<Args>...> data;
};

TEMPLATE_TEST_CASE("Make view of array/slice", "[make_view]", std::uint8_t,
    std::int8_t, std::uint16_t, std::int16_t, std::uint32_t, std::int32_t,
    std::uint64_t, std::int64_t, float, double, Datestamp<DateUnit::Day>,
    Datestamp<DateUnit::Millisecond>, Timestamp<TimeUnit::Second>,
    Timestamp<TimeUnit::Millisecond>, Timestamp<TimeUnit::Microsecond>,
    Timestamp<TimeUnit::Nanosecond>, Time32<TimeUnit::Second>,
    Time32<TimeUnit::Millisecond>, Time32<TimeUnit::Microsecond>,
    Time32<TimeUnit::Nanosecond>, Time64<TimeUnit::Second>,
    Time64<TimeUnit::Millisecond>, Time64<TimeUnit::Microsecond>,
    Time64<TimeUnit::Nanosecond>, ListView<int>, (StructView<int, double>),
    (ListView<StructView<int, double>>), (StructView<ListView<int>, double>) )
{
    using T = TestType;

    std::size_t n = 1000;
    std::int64_t m = 500;

    SECTION("View of array")
    {
        TestData<T> data(n, false);
        auto view = ::dataframe::make_view<T>(data.array);
        data.check(view);
    }

    SECTION("View of slice")
    {
        TestData<T> data(n, false);
        auto view = ::dataframe::make_view<T>(data.array->Slice(m));
        data.check(view, m);
    }

    SECTION("View of nullable array")
    {
        TestData<T> data(n, true);
        auto view = ::dataframe::make_view<T>(data.array);
        data.check(view);
    }

    SECTION("View of nullable slice")
    {
        TestData<T> data(n, true);
        auto view = ::dataframe::make_view<T>(data.array->Slice(m));
        data.check(view, m);
    }
}

} // namespace dataframe
