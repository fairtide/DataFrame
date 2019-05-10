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
#include <random>

template <typename T>
struct Generator {
    using U = ::dataframe::CType<T>;

    static auto get(std::size_t n)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<> rval(-1000, 1000);
        std::vector<U> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.emplace_back(static_cast<U>(rval(rng)));
        }

        return values;
    }
};

template <typename T>
auto generate_data(std::size_t n)
{
    return Generator<T>::get(n);
}

template <>
struct Generator<std::string> {
    static auto get(std::size_t n)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::size_t> rsize(0, 10);
        std::vector<std::string> values;

        for (std::size_t i = 0; i != n; ++i) {
            auto buf = generate_data<std::uint8_t>(rsize(rng));
            values.emplace_back(
                reinterpret_cast<const char *>(buf.data()), buf.size());
        }

        return values;
    }
};

template <typename T>
struct Generator<::dataframe::List<T>> {
    using U = ::dataframe::CType<::dataframe::List<T>>;

    static auto get(std::size_t n)
    {
        std::mt19937_64 rng;
        std::uniform_int_distribution<std::size_t> rsize(0, 10);
        std::vector<U> values;

        for (std::size_t i = 0; i != n; ++i) {
            values.emplace_back(generate_data<T>(rsize(rng)));
        }

        return values;
    }
};

template <typename... Types>
struct Generator<::dataframe::Struct<Types...>> {
    using U = ::dataframe::CType<::dataframe::Struct<Types...>>;

    static auto get(std::size_t n)
    {
        std::vector<U> values(n);
        get<0>(values, std::integral_constant<bool, 0 < nfields>());

        return values;
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N>
    static void get(std::vector<U> &values, std::true_type)
    {
        auto v =
            generate_data<::dataframe::FieldType<N, Types...>>(values.size());

        for (std::size_t i = 0; i != v.size(); ++i) {
            std::get<N>(values[i]) = std::move(v[i]);
        }

        get<N + 1>(values, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    static void get(std::vector<U> &, std::false_type)
    {
    }
};
