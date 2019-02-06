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

#ifndef DATAFRAME_ARRAY_PRIMITIVE_HPP
#define DATAFRAME_ARRAY_PRIMITIVE_HPP

#include <dataframe/array/view.hpp>
#include <arrow/api.h>
#include <type_traits>
#include <vector>

namespace dataframe {

namespace internal {

template <bool, std::size_t>
struct IntegerType;

template <>
struct IntegerType<true, 8> {
    using type = ::arrow::Int8Type;
};

template <>
struct IntegerType<true, 16> {
    using type = ::arrow::Int16Type;
};

template <>
struct IntegerType<true, 32> {
    using type = ::arrow::Int32Type;
};

template <>
struct IntegerType<true, 64> {
    using type = ::arrow::Int64Type;
};

template <>
struct IntegerType<false, 8> {
    using type = ::arrow::UInt8Type;
};

template <>
struct IntegerType<false, 16> {
    using type = ::arrow::UInt16Type;
};

template <>
struct IntegerType<false, 32> {
    using type = ::arrow::UInt32Type;
};

template <>
struct IntegerType<false, 64> {
    using type = ::arrow::UInt64Type;
};

template <typename>
struct FloatingPointType;

template <>
struct FloatingPointType<float> {
    using type = ::arrow::FloatType;
};

template <>
struct FloatingPointType<double> {
    using type = ::arrow::DoubleType;
};

template <typename Type, typename T>
inline std::shared_ptr<::arrow::Array> make_primitive_array(
    const ArrayViewBase<T> &view)
{
    typename ::arrow::TypeTraits<Type>::BuilderType builder(
        ::arrow::default_memory_pool());

    if (view.mask().null_count() == 0) {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(
            view.data(), static_cast<std::int64_t>(view.size())));
    } else {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(view.data(),
            static_cast<std::int64_t>(view.size()), view.mask().data()));
    }

    std::shared_ptr<::arrow::Array> out;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&out));

    return out;
}

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
        view_ = ArrayView<T>(
            static_cast<std::size_t>(array.length()), array.raw_values());

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

template <typename T>
inline std::enable_if_t<std::is_integral_v<T>, std::shared_ptr<::arrow::Array>>
make_array(const ArrayViewBase<T> &view)
{
    if (view.data() == nullptr) {
        return nullptr;
    }

    return internal::make_primitive_array<typename internal::IntegerType<
        std::is_signed_v<T>, CHAR_BIT * sizeof(T)>::type>(view);
}

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>,
    std::shared_ptr<::arrow::Array>>
make_array(const ArrayViewBase<T> &view)
{
    if (view.data() == nullptr) {
        return nullptr;
    }

    return internal::make_primitive_array<
        typename internal::FloatingPointType<T>::type>(view);
}

template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>> view_array(
    const ::arrow::Array &values, ArrayViewBase<T> *out)
{
    internal::PrimitiveViewVisitor<T> visitor;
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    *out = visitor.view();
}

template <typename T, typename Alloc>
inline std::enable_if_t<std::is_arithmetic_v<T>> cast_array(
    const ::arrow::Array &values, std::vector<T, Alloc> *out)
{
    out->resize(static_cast<std::size_t>(values.length()));
    internal::PrimitiveValueVisitor<T> visitor(out->data());
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_PRIMITIVE_HPP
