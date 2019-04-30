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

#include <bsoncxx/stdx/string_view.hpp>

namespace dataframe {

namespace bson {

struct Schema {
    using view = ::bsoncxx::stdx::string_view;

    static view DATA() { return "d"; }
    static view MASK() { return "m"; }
    static view TYPE() { return "t"; }
    static view PARAM() { return "p"; }

    // binary/string/list
    static view OFFSET() { return "o"; }

    // list
    static view LENGTH() { return "l"; }

    // struct
    static view NAME() { return "n"; }
    static view FIELDS() { return "f"; }

    // decimal128
    static view PRECISION() { return "p"; }
    static view SCALE() { return "s"; }

    // dictionary
    static view INDEX() { return "i"; }
    static view DICT() { return "d"; }
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_SCHEMA_HPP
