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

#ifndef DATAFRAME_ARRAY_MAKE_DATETIME_HPP
#define DATAFRAME_ARRAY_MAKE_DATETIME_HPP

#include <dataframe/array/make/primitive.hpp>

namespace dataframe {

namespace internal {

template <typename T>
struct DatetimeArrayMaker {
    template <typename Iter>
    static void append(BuilderType<T> *builder, Iter first, Iter last)
    {
        using I = std::conditional_t<
            std::is_class_v<typename std::iterator_traits<Iter>::value_type>,
            DatetimeValueIterator<T, Iter>, Iter>;

        DF_ARROW_ERROR_HANDLER(builder->AppendValues(I(first), I(last)));
    }
};

} // namespace internal

template <DateUnit Unit>
struct ArrayMaker<Datestamp<Unit>>
    : internal::DatetimeArrayMaker<Datestamp<Unit>> {
};

template <TimeUnit Unit>
struct ArrayMaker<Timestamp<Unit>>
    : internal::DatetimeArrayMaker<Timestamp<Unit>> {
};

template <TimeUnit Unit>
struct ArrayMaker<Time<Unit>> : internal::DatetimeArrayMaker<Time<Unit>> {
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_DATETIME_HPP
