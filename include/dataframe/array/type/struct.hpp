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

template <std::size_t N>
using FieldIndex = std::integral_constant<std::size_t, N>;

template <std::size_t N, typename... Types>
using FieldType = std::tuple_element_t<N, std::tuple<Types...>>;

template <typename T, std::size_t N>
inline std::string field_name(const T *, FieldIndex<N>)
{
    return "Field" + std::to_string(N);
}

template <typename... Types, std::size_t N>
inline const std::tuple_element_t<N, std::tuple<Types...>> &get_field(
    const std::tuple<Types...> &value, FieldIndex<N>)
{
    return std::get<N>(value);
}

template <typename T, typename... Types>
struct NamedStruct {
};

template <typename... Types>
using Struct = NamedStruct<void, Types...>;

template <typename T, typename... Types>
struct TypeTraits<NamedStruct<T, Types...>> {
    using scalar_type = std::tuple<ScalarType<Types>...>;
    using data_type = ::arrow::StructType;
    using array_type = ::arrow::StructArray;
    using builder_type = ::arrow::StructBuilder;

    static std::shared_ptr<data_type> make_data_type()
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        set_field<0>(fields, std::integral_constant<bool, 0 < nfields>());

        return std::make_shared<data_type>(fields);
    }

    static std::unique_ptr<builder_type> make_builder()
    {
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> builders;
        set_builder<0>(builders, std::integral_constant<bool, 0 < nfields>());

        return std::make_unique<builder_type>(make_data_type(),
            ::arrow::default_memory_pool(), std::move(builders));
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N>
    static void set_field(
        std::vector<std::shared_ptr<::arrow::Field>> &fields, std::true_type)
    {
        fields.push_back(::arrow::field(
            field_name(static_cast<const T *>(nullptr), FieldIndex<N>()),
            ::dataframe::make_data_type<FieldType<N, Types...>>()));

        set_field<N + 1>(
            fields, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    static void set_field(
        std::vector<std::shared_ptr<::arrow::Field>> &, std::false_type)
    {
    }

    template <std::size_t N>
    static void set_builder(
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> &builders,
        std::true_type)
    {
        builders.emplace_back(
            ::dataframe::make_builder<FieldType<N, Types...>>());
        set_builder<N + 1>(
            builders, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    static void set_builder(
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> &, std::false_type)
    {
    }
};

template <typename T, typename... Types>
struct IsType<NamedStruct<T, Types...>, ::arrow::StructType> {
    static bool is_type(const ::arrow::StructType &type)
    {
        return is_type<0>(type, std::integral_constant<bool, 0 < nfields>());
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N>
    static bool is_type(const ::arrow::StructType &type, std::true_type)
    {
        return ::dataframe::is_type<FieldType<N, Types...>>(
                   type.child(N)->type()) &&
            is_type<N + 1>(
                type, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    static bool is_type(const ::arrow::StructType &, std::false_type)
    {
        return true;
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_STRUCT_HPP
