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

#ifndef DATAFRAME_TESTS_MAKE_DATA_HPP
#define DATAFRAME_TESTS_MAKE_DATA_HPP

#include <dataframe/array.hpp>
#include <random>

template <typename T>
struct DataMaker {
    using U = ::dataframe::ScalarType<T>;

    template <typename Iter>
    static auto make(std::size_t n, Iter)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<> rval(1000, 2000);
        std::vector<T> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.emplace_back(static_cast<U>(rval(rng)));
        }

        return values;
    }
};

template <typename T, typename Iter>
auto make_data(std::size_t n, Iter mask)
{
    return DataMaker<T>::make(n, mask);
}

template <typename T>
auto make_data(std::size_t n)
{
    auto mask = ::dataframe::repeat(true, n);
    return make_data<T>(n, mask.begin());
}

template <>
struct DataMaker<void> {
    template <typename Iter>
    static auto make(std::size_t n, Iter)
    {
        return std::vector<std::nullptr_t>(n);
    }
};

template <>
struct DataMaker<bool> {
    template <typename Iter>
    static auto make(std::size_t n, Iter)
    {
        std::mt19937_64 rng;
        std::bernoulli_distribution rbit;
        std::vector<bool> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.push_back(rbit(rng));
        }

        return values;
    }
};

template <>
struct DataMaker<std::uint8_t> {
    template <typename Iter>
    static auto make(std::size_t n, Iter)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::uint8_t> rval(32, 126);
        std::vector<std::uint8_t> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.emplace_back(rval(rng));
        }

        return values;
    }
};

template <>
struct DataMaker<std::string> {
    template <typename Iter>
    static auto make(std::size_t n, Iter mask)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::size_t> rsize(1, 10);
        std::vector<std::string> values;

        for (std::size_t i = 0; i != n; ++i, ++mask) {
            if (*mask) {
                auto buf = make_data<std::uint8_t>(rsize(rng));
                values.emplace_back(
                    reinterpret_cast<const char *>(buf.data()), buf.size());
            } else {
                values.emplace_back();
            }
        }

        return values;
    }
};

template <>
struct DataMaker<::dataframe::Bytes> {
    template <typename Iter>
    static auto make(std::size_t n, Iter mask)
    {
        return make_data<std::string>(n, mask);
    }
};

template <typename T>
struct DataMaker<::dataframe::Opaque<T>> {
    template <typename Iter>
    static auto make(std::size_t n, Iter mask)
    {
        return make_data<T>(n, mask);
    }
};

template <typename T>
struct DataMaker<::dataframe::Dict<T>> {
    template <typename Iter>
    static auto make(std::size_t n, Iter mask)
    {
        return make_data<T>(n, mask);
    }
};

template <typename T>
struct DataMaker<::dataframe::List<T>> {
    using U = ::dataframe::ScalarType<::dataframe::List<T>>;

    template <typename Iter>
    static auto make(std::size_t n, Iter mask)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::size_t> rsize(1, 10);
        std::vector<U> values;

        for (std::size_t i = 0; i != n; ++i, ++mask) {
            if (*mask) {
                values.emplace_back(make_data<T>(rsize(rng)));
            } else {
                values.emplace_back();
            }
        }

        return values;
    }
};

template <typename... Types>
struct DataMaker<::dataframe::Struct<Types...>> {
    using U = ::dataframe::ScalarType<::dataframe::Struct<Types...>>;

    template <typename Iter>
    static auto make(std::size_t n, Iter mask)
    {
        std::vector<U> values(n);
        make<0>(values, mask, std::integral_constant<bool, 0 < nfields>());

        return values;
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N, typename Iter>
    static void make(std::vector<U> &values, Iter mask, std::true_type)
    {
        auto v = make_data<::dataframe::FieldType<N, Types...>>(
            values.size(), mask);

        for (std::size_t i = 0; i != v.size(); ++i) {
            std::get<N>(values[i]) = std::move(v[i]);
        }

        make<N + 1>(
            values, mask, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t, typename Iter>
    static void make(std::vector<U> &, Iter, std::false_type)
    {
    }
};

template <typename T, typename... Types>
struct DataMaker<::dataframe::NamedStruct<T, Types...>>
    : DataMaker<::dataframe::Struct<Types...>> {
};

namespace dataframe {

template <DateUnit Unit>
inline bool operator==(
    typename Datestamp<Unit>::value_type v1, const Datestamp<Unit> &v2)
{
    return v1 == v2.value;
}

template <TimeUnit Unit>
inline bool operator==(
    typename Timestamp<Unit>::value_type v1, const Timestamp<Unit> &v2)
{
    return v1 == v2.value;
}

template <TimeUnit Unit>
inline bool operator==(
    typename Time<Unit>::value_type v1, const Time<Unit> &v2)
{
    return v1 == v2.value;
}

template <typename V, typename... Types>
inline bool operator==(const std::tuple<Types...> &v1, V v2)
{
    return v1 == v2.get();
}

template <typename T, typename V>
inline bool operator==(const std::vector<T> &v1, V v2)
{
    return std::equal(v1.begin(), v1.end(), v2.begin(), v2.end());
}

} // namespace dataframe

#endif // DATAFRAME_TESTS_MAKE_DATA_HPP
