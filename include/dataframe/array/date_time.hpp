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

#ifndef DATAFRAME_ARRAY_DATE_TIME_HPP
#define DATAFRAME_ARRAY_DATE_TIME_HPP

#include <dataframe/array/make_array.hpp>
#include <dataframe/array/view.hpp>
#include <dataframe/error.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace dataframe {

using Date = ::boost::gregorian::date;

using Timestamp = ::boost::posix_time::ptime;

namespace internal {

class Date32Visitor : public ::arrow::ArrayVisitor
{
  public:
    Date32Visitor(Date *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        auto n = static_cast<std::size_t>(array.length());
        auto v = array.raw_values();
        Date epoch(1970, 1, 1);
        if (array.null_count() == 0) {
            for (std::size_t i = 0; i != n; ++i) {
                out_[i] = epoch + ::boost::gregorian::days(v[i]);
            }
        } else {
            for (std::size_t i = 0; i != n; ++i) {
                if (array.IsValid(static_cast<std::int64_t>(i))) {
                    out_[i] = epoch + ::boost::gregorian::days(v[i]);
                }
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    Date *out_;
};

class TimestampVisitor : public ::arrow::ArrayVisitor
{
  public:
    TimestampVisitor(Timestamp *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        switch (std::static_pointer_cast<::arrow::TimestampType>(array.type())
                    ->unit()) {
            case ::arrow::TimeUnit::SECOND:
                return visit<::boost::posix_time::seconds>(array);
            case ::arrow::TimeUnit::MILLI:
                return visit<::boost::posix_time::milliseconds>(array);
            case ::arrow::TimeUnit::MICRO:
                return visit<::boost::posix_time::microseconds>(array);
            case ::arrow::TimeUnit::NANO:
#ifdef BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                return visit<::boost::posix_time::nanoseconds>(array);
#else  // BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                auto n = static_cast<std::size_t>(array.length());
                auto v = array.raw_values();
                Timestamp epoch(Date(1970, 1, 1));
                if (array.null_count() == 0) {
                    for (std::size_t i = 0; i != n; ++i) {
                        out_[i] = epoch +
                            ::boost::posix_time::microseconds(v[i] / 1000);
                    }
                } else {
                    for (std::size_t i = 0; i != n; ++i) {
                        if (array.IsValid(static_cast<std::int64_t>(i))) {
                            out_[i] = epoch +
                                ::boost::posix_time::microseconds(v[i] / 1000);
                        }
                    }
                }
                return ::arrow::Status::OK();
#endif // BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
        }
    }

  private:
    template <typename T>
    ::arrow::Status visit(const ::arrow::TimestampArray &array)
    {
        auto n = static_cast<std::size_t>(array.length());
        auto v = array.raw_values();
        Timestamp epoch(Date(1970, 1, 1));
        if (array.null_count() == 0) {
            for (std::size_t i = 0; i != n; ++i) {
                out_[i] = epoch + T(v[i]);
            }
        } else {
            for (std::size_t i = 0; i != n; ++i) {
                if (array.IsValid(static_cast<std::int64_t>(i))) {
                    out_[i] = epoch + T(v[i]);
                }
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    Timestamp *out_;
};

} // namespace internal

inline constexpr bool is_scalar(Date *) { return true; }

inline constexpr bool is_scalar(Timestamp *) { return true; }

inline bool is_convertible(::arrow::Type::type type, Date *)
{
    return type == ::arrow::Type::DATE32;
}

inline bool is_convertible(::arrow::Type::type type, Timestamp *)
{
    return type == ::arrow::Type::TIMESTAMP;
}

inline std::shared_ptr<::arrow::Array> make_array(
    const ArrayViewBase<Date> &view)
{
    if (view.data() == nullptr) {
        return nullptr;
    }

    Date epoch(1970, 1, 1);

    std::vector<std::int32_t> days;
    days.reserve(view.size());
    for (std::size_t i = 0; i != view.size(); ++i) {
        days.emplace_back((view[i] - epoch).days());
    }

    ::arrow::Date32Builder builder(::arrow::default_memory_pool());

    if (view.null_count() == 0) {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(
            days.data(), static_cast<std::int64_t>(days.size())));
    } else {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(
            days.data(), static_cast<std::int64_t>(days.size()), view.mask()));
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

inline std::shared_ptr<::arrow::Array> make_array(
    const ArrayViewBase<Timestamp> &view)
{
    if (view.data() == nullptr) {
        return nullptr;
    }

    Timestamp epoch(Date(1970, 1, 1));

    std::vector<std::int64_t> nanos;
    nanos.reserve(view.size());
    for (std::size_t i = 0; i != view.size(); ++i) {
        nanos.emplace_back((view[i] - epoch).total_nanoseconds());
    }

    auto type =
        std::make_shared<::arrow::TimestampType>(::arrow::TimeUnit::NANO);

    ::arrow::TimestampBuilder builder(type, ::arrow::default_memory_pool());

    if (view.null_count() == 0) {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(
            nanos.data(), static_cast<std::int64_t>(nanos.size())));
    } else {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(nanos.data(),
            static_cast<std::int64_t>(nanos.size()), view.mask()));
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

enum class TimeUnit { Day, Second, Millisecond, Microsecond, Nanosecond };

inline std::shared_ptr<::arrow::Array> make_array(
    const ArrayViewBase<std::int64_t> &view, TimeUnit unit)
{
    if (view.data() == nullptr) {
        return nullptr;
    }

    switch (unit) {
        case TimeUnit::Day: {
            std::vector<std::int32_t> dates(
                view.data(), view.data() + view.size());
            return make_array(
                ArrayView<std::int32_t>(dates.size(), dates.data()),
                std::make_shared<::arrow::Date32Type>());
        }
        case TimeUnit::Second:
            return make_array(view,
                std::make_shared<::arrow::TimestampType>(
                    ::arrow::TimeUnit::SECOND));
        case TimeUnit::Millisecond:
            return make_array(view,
                std::make_shared<::arrow::TimestampType>(
                    ::arrow::TimeUnit::MILLI));
        case TimeUnit::Microsecond:
            return make_array(view,
                std::make_shared<::arrow::TimestampType>(
                    ::arrow::TimeUnit::MICRO));
        case TimeUnit::Nanosecond:
            return make_array(view,
                std::make_shared<::arrow::TimestampType>(
                    ::arrow::TimeUnit::NANO));
    }
}

template <typename Alloc>
inline void cast_array(
    const ::arrow::Array &values, std::vector<Date, Alloc> *out)
{
    out->resize(static_cast<std::size_t>(values.length()));
    internal::Date32Visitor visitor(out->data());
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

template <typename Alloc>
inline void cast_array(
    const ::arrow::Array &values, std::vector<Timestamp, Alloc> *out)
{
    out->resize(static_cast<std::size_t>(values.length()));
    internal::TimestampVisitor visitor(out->data());
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_DATE_TIME_HPP
