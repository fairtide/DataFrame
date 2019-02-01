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

#ifndef DATAFRAME_ARRAY_INTERNAL_PRIMITIVE_VISITOR_HPP
#define DATAFRAME_ARRAY_INTERNAL_PRIMITIVE_VISITOR_HPP

#include <dataframe/array/view.hpp>
#include <arrow/api.h>

namespace dataframe {

namespace internal {

template <typename T>
class PrimitiveViewVisitor : public ::arrow::ArrayVisitor
{
  public:
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

    ::arrow::Status Visit(const ::arrow::FloatArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Time32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Time64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return visit(array);
    }

    ArrayView<T> view() const { return view_; }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        return visit(array, std::is_same<T, typename ArrayType::value_type>());
    }

    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array, std::true_type)
    {
        auto n = static_cast<std::size_t>(array.length());
        if (array.null_count() == 0) {
            view_ = ArrayView<T>(n, array.raw_values());
        } else {
            auto v = array.null_bitmap_data();
            std::vector<bool> is_valid(n);
            if (v != nullptr) {
                for (std::size_t i = 0; i != n; ++i) {
                    is_valid[i] = v[i / 8] & (1 << (i % 8));
                }
            }
            view_ = ArrayView<T>(n, array.raw_values(), std::move(is_valid));
        }

        return ::arrow::Status::OK();
    }

    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array, std::false_type)
    {
        return ::arrow::Status::Invalid("Array of type " +
            array.type()->name() +
            " cannot be viewed as the destination type");
    }

  private:
    ArrayView<T> view_;
};

template <typename T>
class PrimitiveValueVisitor : public ::arrow::ArrayVisitor
{
  public:
    PrimitiveValueVisitor(T *out)
        : out_(out)
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

    ::arrow::Status Visit(const ::arrow::FloatArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Time32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Time64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return visit(array);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        std::copy_n(array.raw_values(), array.length(), out_);

        return ::arrow::Status::OK();
    }

  private:
    T *out_;
};

} // namespace internal

} // namespace dataframe

#endif // DATAFRAME_ARRAY_INTERNAL_PRIMITIVE_VISITOR_HPP
