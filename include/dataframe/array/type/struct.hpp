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

#ifndef DATAFRAME_ARRAY_TYPE_STRUCT_HPP
#define DATAFRAME_ARRAY_TYPE_STRUCT_HPP

#include <dataframe/array/type/primitive.hpp>
#include <tuple>

namespace dataframe {

struct StructBase {
};

template <typename... Types>
struct Struct final : StructBase {
    using data_type = std::tuple<Types...>;
};

template <std::size_t N, typename... Types>
using FieldType = std::tuple_element_t<N, std::tuple<Types...>>;

template <typename... Types>
struct TypeTraits<Struct<Types...>> {
    static std::shared_ptr<::arrow::StructType> data_type()
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;

        set_field<0>(fields, std::integral_constant<bool, 0 < nfields>());

        auto ret = ::arrow::struct_(fields);

        return std::static_pointer_cast<::arrow::StructType>(ret);
    }

    template <typename Name1, typename... Names>
    static std::shared_ptr<::arrow::StructType> data_type(
        Name1 &&name1, Names &&... names)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;

        set_field<0>(fields, std::integral_constant<bool, 0 < nfields>(),
            std::forward<Name1>(name1), std::forward<Names>(names)...);

        auto ret = ::arrow::struct_(fields);

        return std::static_pointer_cast<::arrow::StructType>(ret);
    }

    template <typename... Args>
    static std::unique_ptr<::arrow::StructBuilder> builder(Args &&... args)
    {
        return std::make_unique<::arrow::StructBuilder>(
            data_type(std::forward<Args>(args)...),
            ::arrow::default_memory_pool(), field_builders());
    }

    using ctype = std::tuple<CType<Types>...>;
    using array_type = ::arrow::StructArray;

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t>
    static void set_field(
        std::vector<std::shared_ptr<::arrow::Field>> &, std::false_type)
    {
    }

    template <std::size_t N>
    static void set_field(
        std::vector<std::shared_ptr<::arrow::Field>> &fields, std::true_type)
    {
        using T = FieldType<N, Types...>;

        fields.push_back(::arrow::field(std::string(), make_data_type<T>()));

        set_field<N + 1>(
            fields, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t N, typename Name1, typename... Names>
    static void set_field(std::vector<std::shared_ptr<::arrow::Field>> &fields,
        std::true_type, Name1 &&name1, Names &&... names)
    {
        using T = FieldType<N, Types...>;

        fields.push_back(
            ::arrow::field(std::forward<Name1>(name1), make_data_type<T>()));

        set_field<N + 1>(fields,
            std::integral_constant<bool, N + 1 < nfields>(),
            std::forward<Names>(names)...);
    }

    static std::vector<std::shared_ptr<::arrow::ArrayBuilder>> field_builders()
    {
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> builders;
        set_builder<0>(builders, std::integral_constant<bool, 0 < nfields>());

        return builders;
    }

    template <std::size_t>
    static void set_builder(
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> &, std::false_type)
    {
    }

    template <std::size_t N>
    static void set_builder(
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> &builders,
        std::true_type)
    {
        builders.emplace_back(make_builder<FieldType<N, Types...>>());
        set_builder<N + 1>(
            builders, std::integral_constant<bool, N + 1 < nfields>());
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_STRUCT_HPP
