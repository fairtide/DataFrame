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

#ifndef DATAFRAME_ARRAY_TYPE_LIST_HPP
#define DATAFRAME_ARRAY_TYPE_LIST_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

class ListBase
{
};

template <typename T>
class List : public ListBase
{
  public:
    using value_type = T;
    // TODO
};

template <typename T>
struct TypeTraits<List<T>> {
    static std::shared_ptr<::arrow::ListType> data_type()
    {
        auto ret = ::arrow::list(make_data_type<T>());

        return std::static_pointer_cast<::arrow::ListType>(ret);
    }

    static std::unique_ptr<::arrow::ListBuilder> builder()
    {
        return std::make_unique<::arrow::ListBuilder>(
            ::arrow::default_memory_pool(), make_builder<T>(), data_type());
    }

    using array_type = ::arrow::ListArray;
};

template <typename T>
class ListView;

template <typename T>
struct TypeTraits<ListView<T>> : TypeTraits<List<T>> {
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_LIST_HPP
