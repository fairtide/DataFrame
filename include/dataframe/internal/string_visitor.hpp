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

#ifndef DATAFRAME_INTERNAL_STRING_VISITOR_HPP
#define DATAFRAME_INTERNAL_STRING_VISITOR_HPP

#include <dataframe/internal/categorical_visitor.hpp>
#include <arrow/api.h>

namespace dataframe {

namespace internal {

template <typename T>
class StringVisitor : public ::arrow::ArrayVisitor
{
  public:
    StringVisitor(T *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::BinaryArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) final
    {
        CategoricalVisitor<T> visitor(out_);
        return array.Accept(&visitor);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        auto n = array.length();
        out_->clear();
        out_->reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            if (array.IsValid(i)) {
                out_->emplace_back(array.GetView(i));
            } else {
                out_->emplace_back();
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    T *out_;
};

} // namespace internal

} // namespace dataframe

#endif // DATAFRAME_INTERNAL_STRING_VISITOR_HPP
