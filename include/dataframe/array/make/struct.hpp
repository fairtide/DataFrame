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
    static void append(
        BuilderType<Struct<Types...>> *builder, Iter first, Iter last)
    {
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(
            static_cast<std::int64_t>(std::distance(first, last)), nullptr));

        append<0>(
            builder, first, last, std::integral_constant<bool, 0 < nfields>());
    }

  private:
    template <std::size_t N, typename V, typename R, typename Iter>
    class iterator
    {
      public:
        using value_type = V;
        using reference = R;
        using iterator_category =
            typename std::iterator_traits<Iter>::iterator_category;

        iterator(Iter iter)
            : iter_(iter)
        {
        }

        reference operator*() const
        {
            return get_field(*iter_, FieldIndex<N>());
        }

        DF_DEFINE_ITERATOR_MEMBERS(iterator, iter_)

      private:
        Iter iter_;
    };

    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t N, typename Iter>
    static void append(BuilderType<Struct<Types...>> *builder, Iter first,
        Iter last, std::true_type)
    {
        using T = FieldType<N, Types...>;
        using R = decltype(get_field(*first, FieldIndex<N>()));
        using V = std::remove_cv_t<std::remove_reference_t<R>>;
        using I = iterator<N, V, R, Iter>;

        auto field_builder =
            dynamic_cast<BuilderType<T> *>(builder->field_builder(N));

        if (field_builder == nullptr) {
            throw DataFrameException("Null field builder");
        }

        ArrayMaker<T>::append(field_builder, I(first), I(last));

        append<N + 1>(builder, first, last,
            std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t, typename Iter>
    static void append(
        BuilderType<Struct<Types...>> *, Iter, Iter, std::false_type)
    {
    }
};

template <typename T, typename... Types>
struct ArrayMaker<NamedStruct<T, Types...>>
    : public ArrayMaker<Struct<Types...>> {
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_STRUCT_HPP
