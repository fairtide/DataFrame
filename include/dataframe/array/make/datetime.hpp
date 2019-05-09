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

#include <dataframe/array/make/iterator.hpp>
#include <dataframe/array/make/primitive.hpp>

namespace dataframe {

namespace internal {

template <typename T>
struct DatetimeArrayMaker {
    template <typename Iter>
    static std::shared_ptr<::arrow::Array> make(Iter first, Iter last)
    {
        auto builder = make_builder<T>();

        DF_ARROW_ERROR_HANDLER(
            builder->AppendValues(field_iterator(first, &T::value),
                field_iterator(last, &T::value)));

        std::shared_ptr<::arrow::Array> ret;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&ret));

        return ret;
    }

  private:
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
struct ArrayMaker<Time32<Unit>> : internal::DatetimeArrayMaker<Time32<Unit>> {
};

template <TimeUnit Unit>
struct ArrayMaker<Time64<Unit>> : internal::DatetimeArrayMaker<Time64<Unit>> {
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_DATETIME_HPP
