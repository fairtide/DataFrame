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

#ifndef DATAFRAME_SELECT_HPP
#define DATAFRAME_SELECT_HPP

#include <dataframe/data_frame.hpp>

namespace dataframe {

namespace internal {

template <typename InputIter>
class SelectVisitor final : public ::arrow::ArrayVisitor
{
  public:
    std::shared_ptr<::arrow::Array> values;

    explicit SelectVisitor(InputIter begin, InputIter end)
        : begin_(begin)
        , end_(end)
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

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        ::arrow::StringBuilder builder(::arrow::default_memory_pool());

        auto begin = begin_;
        auto end = end_;
        auto n = array.length();
        ARROW_RETURN_NOT_OK(builder.Reserve(std::distance(begin, end)));

        while (begin != end) {
            auto i = static_cast<std::int64_t>(*begin++);
            if (i >= n) {
                return ::arrow::Status::Invalid("Out of range select index");
            }

            if (i >= 0 && array.IsValid(i)) {
                ARROW_RETURN_NOT_OK(builder.Append(array.GetView(i)));
            } else {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            }
        }

        return builder.Finish(&values);
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) final
    {
        SelectVisitor<InputIter> visitor(begin_, end_);
        ARROW_RETURN_NOT_OK(array.indices()->Accept(&visitor));

        return ::arrow::DictionaryArray::FromArrays(
            array.type(), visitor.values, &values);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        typename ::arrow::TypeTraits<
            typename ArrayType::TypeClass>::BuilderType builder(array.type(),
            ::arrow::default_memory_pool());

        auto begin = begin_;
        auto end = end_;
        auto n = array.length();
        auto v = array.raw_values();
        ARROW_RETURN_NOT_OK(builder.Reserve(std::distance(begin, end)));

        while (begin != end) {
            auto i = static_cast<std::int64_t>(*begin++);
            if (i >= n) {
                return ::arrow::Status::Invalid("Out of range select index");
            }

            if (i >= 0 && array.IsValid(i)) {
                ARROW_RETURN_NOT_OK(builder.Append(v[i]));
            } else {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            }
        }

        return builder.Finish(&values);
    }

  private:
    InputIter begin_;
    InputIter end_;
};

class SelectMaskVisitor final : public ::arrow::ArrayVisitor
{
  public:
    std::shared_ptr<::arrow::Array> values;

    explicit SelectMaskVisitor(const ArrayMask &mask, std::int64_t hint)
        : mask_(mask)
        , hint_(hint)
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

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        ::arrow::StringBuilder builder(::arrow::default_memory_pool());

        auto n = array.length();
        ARROW_RETURN_NOT_OK(builder.Reserve(hint_));

        for (std::int64_t i = 0; i != n; ++i) {
            if (mask_[static_cast<std::size_t>(i)]) {
                if (array.IsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(array.GetView(i)));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
        }

        return builder.Finish(&values);
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) final
    {
        SelectMaskVisitor visitor(mask_, hint_);
        ARROW_RETURN_NOT_OK(array.indices()->Accept(&visitor));

        return ::arrow::DictionaryArray::FromArrays(
            array.type(), visitor.values, &values);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        typename ::arrow::TypeTraits<
            typename ArrayType::TypeClass>::BuilderType builder(array.type(),
            ::arrow::default_memory_pool());

        auto n = array.length();
        auto v = array.raw_values();
        ARROW_RETURN_NOT_OK(builder.Reserve(hint_));

        for (std::int64_t i = 0; i != n; ++i) {
            if (mask_[static_cast<std::size_t>(i)]) {
                if (array.IsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(v[i]));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
        }

        return builder.Finish(&values);
    }

  private:
    const ArrayMask &mask_;
    std::int64_t hint_;
};

} // namespace internal

template <typename InputIter>
inline DataFrame select(const DataFrame &df, InputIter begin, InputIter end)
{
    DataFrame ret;
    auto ncol = df.ncol();
    for (std::size_t i = 0; i != ncol; ++i) {
        auto col = df[i];
        internal::SelectVisitor<InputIter> visitor(begin, end);
        DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));
        ret[col.name()] = std::move(visitor.values);
    }

    return ret;
}

inline DataFrame select(const DataFrame &df, const ArrayMask &mask)
{
    if (df.nrow() != mask.size()) {
        throw DataFrameException("mask does not have the correct size");
    }

    auto hint = static_cast<std::int64_t>(df.nrow() - mask.null_count());

    DataFrame ret;
    auto ncol = df.ncol();
    for (std::size_t i = 0; i != ncol; ++i) {
        auto col = df[i];
        internal::SelectMaskVisitor visitor(mask, hint);
        DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));
        ret[col.name()] = std::move(visitor.values);
    }

    return ret;
}

} // namespace dataframe

#endif // DATAFRAME_SELECT_HPP
