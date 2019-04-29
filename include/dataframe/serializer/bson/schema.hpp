//============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//============================================================================

#ifndef DATAFRAME_SERIALIZER_BSON_SCHEMA_HPP
#define DATAFRAME_SERIALIZER_BSON_SCHEMA_HPP

#include <arrow/allocator.h>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <vector>

namespace dataframe {

namespace bson {

struct Schema {
    using view = ::bsoncxx::stdx::string_view;

    static view DATA() { return "d"; }
    static view MASK() { return "m"; }
    static view TYPE() { return "t"; }
    static view OFFSET() { return "p"; }
    static view PARAM() { return "o"; }
}; // namespace bsonstructSchema

template <typename T>
class Allocator : public ::arrow::stl_allocator<T>
{
  public:
    template <typename U>
    void construct(U *p)
    {
        construct_dispatch(p,
            std::integral_constant<bool,
                (std::is_scalar<U>::value || std::is_pod<U>::value)>());
    }

  private:
    template <typename U>
    void construct_dispatch(U *, std::true_type)
    {
    }

    template <typename U>
    void construct_dispatch(U *p, std::false_type)
    {
        ::new (static_cast<void *>(p)) U();
    }
};

template <typename T>
using Vector = std::vector<T, Allocator<T>>;

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_SCHEMA_HPP
