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

#include <dataframe/internal/date_time_visitor.hpp>
#include <dataframe/array/view.hpp>
#include <dataframe/error.hpp>

namespace dataframe {

inline std::shared_ptr<::arrow::Array> make_array(
    const ArrayView<::boost::gregorian::date> &view)
{
    ::boost::gregorian::date epoch(1970, 1, 1);

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
    const ArrayView<::boost::posix_time::ptime> &view)
{
    ::boost::posix_time::ptime epoch(::boost::gregorian::date(1970, 1, 1));

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

template <typename Alloc>
inline void cast_array(const ::arrow::Array &values,
    std::vector<::boost::gregorian::date, Alloc> *out)
{
    out->resize(static_cast<std::size_t>(values.length()));
    internal::Date32Visitor visitor(out->data());
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

inline constexpr bool is_scalar(::boost::gregorian::date *) { return true; }

bool is_convertible(::arrow::Type::type type, ::boost::gregorian::date *)
{
    return type == ::arrow::Type::DATE32;
}

template <typename Alloc>
inline void cast_array(const ::arrow::Array &values,
    std::vector<::boost::posix_time::ptime, Alloc> *out)
{
    out->resize(static_cast<std::size_t>(values.length()));
    internal::TimestampVisitor visitor(out->data());
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

inline constexpr bool is_scalar(::boost::posix_time::ptime *) { return true; }

bool is_convertible(::arrow::Type::type type, ::boost::posix_time::ptime *)
{
    return type == ::arrow::Type::TIMESTAMP;
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_DATE_TIME_HPP
