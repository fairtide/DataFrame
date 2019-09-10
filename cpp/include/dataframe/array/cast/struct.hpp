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

#ifndef DATAFRAME_ARRAY_CAST_STRUCT_HPP
#define DATAFRAME_ARRAY_CAST_STRUCT_HPP

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

template <typename... Types>
struct CastArrayVisitor<Struct<Types...>> : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    ::arrow::MemoryPool *pool;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *p)
        : result(std::move(data))
        , pool(p)
    {
    }

    ::arrow::Status Visit(const ::arrow::StructArray &array) override
    {
        auto &type = static_cast<const ::arrow::StructType &>(*array.type());

        if (nfields != type.num_children()) {
            throw DataFrameException("Structure of wrong size");
        }

        auto data = array.data()->Copy();
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        std::vector<std::shared_ptr<::arrow::ArrayData>> children;

        cast<0>(array, type, fields, children,
            std::integral_constant<bool, 0 < nfields>());

        data->child_data = std::move(children);

        data->type = ::arrow::struct_(fields);

        result = ::arrow::MakeArray(std::move(data));

        return ::arrow::Status::OK();
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N>
    void cast(const ::arrow::StructArray &array,
        const ::arrow::StructType &type,
        std::vector<std::shared_ptr<::arrow::Field>> &fields,
        std::vector<std::shared_ptr<::arrow::ArrayData>> &children,
        std::true_type)
    {
        children.push_back(
            cast_array<FieldType<N, Types...>>(array.field(N), pool)
                ->data()
                ->Copy());

        fields.push_back(
            ::arrow::field(type.child(N)->name(), children.back()->type));

        cast<N + 1>(array, type, fields, children,
            std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    void cast(const ::arrow::StructArray &, const ::arrow::StructType &,
        std::vector<std::shared_ptr<::arrow::Field>> &,
        std::vector<std::shared_ptr<::arrow::ArrayData>> &, std::false_type)
    {
    }
};

template <typename T, typename... Types>
struct CastArrayVisitor<NamedStruct<T, Types...>>
    : CastArrayVisitor<Struct<Types...>> {
    using CastArrayVisitor<Struct<Types...>>::CastArrayVisitor;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_STRUCT_HPP
