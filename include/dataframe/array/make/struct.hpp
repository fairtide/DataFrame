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

#ifndef DATAFRAME_ARRAY_MAKE_STRUCT_HPP
#define DATAFRAME_ARRAY_MAKE_STRUCT_HPP

#include <dataframe/array/make/primitive.hpp>

namespace dataframe {

template <typename... Types>
struct ArrayMaker<Struct<Types...>> {
    template <typename Iter>
    static std::shared_ptr<::arrow::Array> make(Iter first, Iter last)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        std::vector<std::shared_ptr<::arrow::Array>> arrays;
        make<0>(fields, arrays, first, last,
            std::integral_constant<bool, 0 < nfields>());

        if (arrays.empty()) {
            return nullptr;
        }

        return std::make_shared<::arrow::StructArray>(
            ::arrow::struct_(fields), arrays.front()->length(), arrays);
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N, typename Iter>
    static void make(std::vector<std::shared_ptr<::arrow::Field>> &fields,
        std::vector<std::shared_ptr<::arrow::Array>> &arrays, Iter first,
        Iter last, std::true_type)
    {
        using T = FieldType<N, Types...>;

        constexpr std::integral_constant<std::size_t, N> index;

        arrays.emplace_back(make_array<T>(
            field_iterator(first, index), field_iterator(last, index)));

        fields.emplace_back(
            ::arrow::field(field_name(first, index), arrays.back()->type()));

        make<N + 1>(fields, arrays, first, last,
            std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t, typename Iter>
    static void make(std::vector<std::shared_ptr<::arrow::Field>> &,
        std::vector<std::shared_ptr<::arrow::Array>> &, Iter, Iter,
        std::false_type)
    {
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_STRUCT_HPP
