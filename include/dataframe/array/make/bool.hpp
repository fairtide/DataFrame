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

#ifndef DATAFRAME_ARRAY_MAKE_BOOL_HPP
#define DATAFRAME_ARRAY_MAKE_BOOL_HPP

#include <dataframe/array/make/primitive.hpp>

namespace dataframe {

template <>
struct ArrayMaker<bool> {
    template <typename Iter>
    static void append(BuilderType<bool> *builder, Iter first, Iter last)
    {
        for (auto iter = first; iter != last; ++iter) {
            DF_ARROW_ERROR_HANDLER(builder->Append(static_cast<bool>(*iter)));
        }
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_BOOL_HPP
