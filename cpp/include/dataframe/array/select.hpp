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

#ifndef DATAFRAME_ARRAY_SELECT_HPP
#define DATAFRAME_ARRAY_SELECT_HPP

#include <dataframe/array/make.hpp>
#include <dataframe/array/view.hpp>

namespace dataframe {

template <typename Iter>
std::shared_ptr<::arrow::Array> select_array(
    const std::shared_ptr<::arrow::Array> &array, Iter first, Iter last);

namespace internal {

template <typename T, typename Iter>
class SelectIterator
{
  public:
    using value_type = typename ArrayView<T>::value_type;

    using reference = std::conditional_t<
        std::is_reference_v<typename ArrayView<T>::reference>,
        typename ArrayView<T>::value_type, typename ArrayView<T>::reference>;

    using iterator_category =
        typename std::iterator_traits<Iter>::iterator_category;

    SelectIterator(const ArrayView<T> &view, Iter iter)
        : view_(view)
        , iter_(iter)
    {
    }

    reference operator*() const noexcept
    {
        auto idx = *iter_;

        if (idx < 0) {
            return reference{};
        } else {
            return view_.at(static_cast<std::size_t>(idx));
        }
    }

    DF_DEFINE_ITERATOR_MEMBERS(SelectIterator, iter_)

  private:
    const ArrayView<T> &view_;
    Iter iter_;
};

template <typename Iter>
struct SelectVisitor : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> values;
    std::shared_ptr<::arrow::Array> result;
    Iter first;
    Iter last;
    std::int64_t length;

    SelectVisitor(std::shared_ptr<::arrow::Array> v, Iter f, Iter l)
        : values(std::move(v))
        , first(f)
        , last(l)
    {
    }

    ::arrow::Status Visit(const ::arrow::NullArray &) override
    {
        result = std::make_shared<::arrow::NullArray>(
            static_cast<std::int64_t>(std::distance(first, last)));

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(Arrow, T)                                           \
    ::arrow::Status Visit(const ::arrow::Arrow##Array &) override             \
    {                                                                         \
        using I = SelectIterator<T, Iter>;                                    \
        auto view = make_view<T>(values);                                     \
        result = make_array<T>(I(view, first), I(view, last));                \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Boolean, bool)
    DF_DEFINE_VISITOR(Int8, std::int8_t)
    DF_DEFINE_VISITOR(Int16, std::int16_t)
    DF_DEFINE_VISITOR(Int32, std::int32_t)
    DF_DEFINE_VISITOR(Int64, std::int64_t)
    DF_DEFINE_VISITOR(UInt8, std::uint8_t)
    DF_DEFINE_VISITOR(UInt16, std::uint16_t)
    DF_DEFINE_VISITOR(UInt32, std::uint32_t)
    DF_DEFINE_VISITOR(UInt64, std::uint64_t)
    DF_DEFINE_VISITOR(Float, float)
    DF_DEFINE_VISITOR(Double, double)
    DF_DEFINE_VISITOR(String, std::string)
    DF_DEFINE_VISITOR(Binary, std::string)
    DF_DEFINE_VISITOR(Date32, Datestamp<DateUnit::Day>)
    DF_DEFINE_VISITOR(Date64, Datestamp<DateUnit::Millisecond>)

#undef DF_DEFINE_VISITOR

    // DF_DFINE_VISITOR(HalfFloatArray &);
    // DF_DFINE_VISITOR(FixedSizeBinaryArray &);

    ::arrow::Status Visit(const ::arrow::Time32Array &array) override
    {
        auto type =
            std::static_pointer_cast<::arrow::Time32Type>(array.type());

        switch (type->unit()) {
            case ::arrow::TimeUnit::SECOND: {
                using T = Time<TimeUnit::Second>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
            case ::arrow::TimeUnit::MILLI: {
                using T = Time<TimeUnit::Millisecond>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
            case ::arrow::TimeUnit::MICRO:
                throw DataFrameException("Unexpected Time32 unit");
            case ::arrow::TimeUnit::NANO:
                throw DataFrameException("Unexpected Time32 unit");
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Time64Array &array) override
    {
        auto type =
            std::static_pointer_cast<::arrow::Time64Type>(array.type());

        switch (type->unit()) {
            case ::arrow::TimeUnit::SECOND:
                throw DataFrameException("Unexpected Time64 unit");
            case ::arrow::TimeUnit::MILLI:
                throw DataFrameException("Unexpected Time64 unit");
            case ::arrow::TimeUnit::MICRO: {
                using T = Time<TimeUnit::Microsecond>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
            case ::arrow::TimeUnit::NANO: {
                using T = Time<TimeUnit::Nanosecond>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
    {
        auto type =
            std::static_pointer_cast<::arrow::TimestampType>(array.type());

        switch (type->unit()) {
            case ::arrow::TimeUnit::SECOND: {
                using T = Timestamp<TimeUnit::Second>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
            case ::arrow::TimeUnit::MILLI: {
                using T = Timestamp<TimeUnit::Millisecond>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
            case ::arrow::TimeUnit::MICRO: {
                using T = Timestamp<TimeUnit::Microsecond>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
            case ::arrow::TimeUnit::NANO: {
                using T = Timestamp<TimeUnit::Nanosecond>;
                using I = SelectIterator<T, Iter>;
                auto view = make_view<T>(values);
                result = make_array<T>(I(view, first), I(view, last));
            } break;
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) override
    {
        auto index = select_array(array.indices(), first, last);

        return ::arrow::DictionaryArray::FromArrays(
            array.type(), index, array.dictionary(), &result);
    }
};

} // namespace internal

template <typename Iter>
std::shared_ptr<::arrow::Array> select_array(
    const std::shared_ptr<::arrow::Array> &array, Iter first, Iter last)
{
    internal::SelectVisitor<Iter> visitor(array, first, last);
    DF_ARROW_ERROR_HANDLER(array->Accept(&visitor));

    std::vector<bool> valid;
    bool null_count = 0;
    for (auto iter = first; iter != last; ++iter) {
        auto is_valid = *iter >= 0;
        valid.push_back(is_valid);
        null_count += !is_valid;
    }

    return null_count == 0 ? visitor.result :
                             internal::set_mask(visitor.result, valid.begin());
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_SELECT_HPP
