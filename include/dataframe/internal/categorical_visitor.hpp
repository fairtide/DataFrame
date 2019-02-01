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

#ifndef DATAFRAME_ARRAY_INTERNAL_CATEGORICAL_VISITOR_HPP
#define DATAFRAME_ARRAY_INTERNAL_CATEGORICAL_VISITOR_HPP

#include <arrow/api.h>

namespace dataframe {

namespace internal {

template <typename T>
class CategoricalIndexVisitor : public ::arrow::ArrayVisitor
{
  public:
    CategoricalIndexVisitor(const ::arrow::StringArray &levels, T *out)
        : levels_(levels)
        , out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::Int8Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int16Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt8Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt16Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt64Array &array) final
    {
        return visit(array);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        auto n = array.length();
        auto v = array.raw_values();
        out_->clear();
        out_->reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            if (array.IsValid(i)) {
                out_->emplace_back(
                    levels_.GetView(static_cast<std::int64_t>(v[i])));
            } else {
                out_->emplace_back();
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    const ::arrow::StringArray &levels_;
    T *out_;
};

template <typename T>
class CategoricalLevelVisitor : public ::arrow::ArrayVisitor
{
  public:
    CategoricalLevelVisitor(const ::arrow::Array &index, T *out)
        : index_(index)
        , out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        CategoricalIndexVisitor<T> visitor(array, out_);

        return index_.Accept(&visitor);
    }

  private:
    const ::arrow::Array &index_;
    T *out_;
};

template <typename T>
class CategoricalVisitor : public ::arrow::ArrayVisitor
{
  public:
    CategoricalVisitor(T *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) final
    {
        CategoricalLevelVisitor visitor(*array.indices(), out_);

        return array.dictionary()->Accept(&visitor);
    }

  private:
    T *out_;
};

} // namespace internal

} // namespace dataframe

#endif // DATAFRAME_ARRAY_INTERNAL_CATEGORICAL_VISITOR_HPP
