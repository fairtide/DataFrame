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
#include <dataframe/array/view.hpp>
#include <random>

template <typename T>
struct DataMaker {
    using U = ::dataframe::CType<T>;

    static auto make(std::size_t n)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<> rval(-1000, 1000);
        std::vector<T> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.emplace_back(static_cast<U>(rval(rng)));
        }

        return values;
    }
};

template <typename T>
auto make_data(std::size_t n)
{
    return DataMaker<T>::make(n);
}

template <>
struct DataMaker<bool> {
    static auto make(std::size_t n)
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
struct DataMaker<std::string> {
    static auto make(std::size_t n)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::size_t> rsize(1, 10);
        std::vector<std::string> values;

        for (std::size_t i = 0; i != n; ++i) {
            auto buf = make_data<std::uint8_t>(rsize(rng));
            values.emplace_back(
                reinterpret_cast<const char *>(buf.data()), buf.size());
        }

        return values;
    }
};

template <typename T>
struct DataMaker<::dataframe::Dict<T>> {
    static auto make(std::size_t n) { return make_data<T>(n); }
};

template <typename T>
struct DataMaker<::dataframe::List<T>> {
    using U = ::dataframe::CType<::dataframe::List<T>>;

    static auto make(std::size_t n)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::size_t> rsize(1, 10);
        std::vector<U> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.emplace_back(make_data<T>(rsize(rng)));
        }

        return values;
    }
};

template <typename... Types>
struct DataMaker<::dataframe::Struct<Types...>> {
    using U = ::dataframe::CType<::dataframe::Struct<Types...>>;

    static auto make(std::size_t n)
    {
        std::vector<U> values(n);
        make<0>(values, std::integral_constant<bool, 0 < nfields>());

        return values;
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N>
    static void make(std::vector<U> &values, std::true_type)
    {
        auto v = make_data<::dataframe::FieldType<N, Types...>>(values.size());

        for (std::size_t i = 0; i != v.size(); ++i) {
            std::get<N>(values[i]) = std::move(v[i]);
        }

        make<N + 1>(values, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    static void make(std::vector<U> &, std::false_type)
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
