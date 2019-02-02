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

#ifndef DATAFRAME_INTERNAL_DATE_TIME_VISITOR_HPP
#define DATAFRAME_INTERNAL_DATE_TIME_VISITOR_HPP

#include <arrow/api.h>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace dataframe {

namespace internal {

class Date32Visitor : public ::arrow::ArrayVisitor
{
  public:
    Date32Visitor(::boost::gregorian::date *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        auto n = static_cast<std::size_t>(array.length());
        auto v = array.raw_values();
        ::boost::gregorian::date epoch(1970, 1, 1);
        if (array.null_count() == 0) {
            for (std::size_t i = 0; i != n; ++i) {
                out_[i] = epoch + ::boost::gregorian::days(v[i]);
            }
        } else {
            for (std::size_t i = 0; i != n; ++i) {
                if (array.IsValid(i)) {
                    out_[i] = epoch + ::boost::gregorian::days(v[i]);
                }
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    ::boost::gregorian::date *out_;
};

class TimestampVisitor : public ::arrow::ArrayVisitor
{
  public:
    TimestampVisitor(::boost::posix_time::ptime *out)
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
                ::boost::posix_time::ptime epoch(
                    ::boost::gregorian::date(1970, 1, 1));
                if (array.null_count() == 0) {
                    for (std::size_t i = 0; i != n; ++i) {
                        out_[i] = epoch +
                            ::boost::posix_time::microseconds(v[i] / 1000);
                    }
                } else {
                    for (std::size_t i = 0; i != n; ++i) {
                        if (array.IsValid(i)) {
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
        ::boost::posix_time::ptime epoch(::boost::gregorian::date(1970, 1, 1));
        if (array.null_count() == 0) {
            for (std::size_t i = 0; i != n; ++i) {
                out_[i] = epoch + T(v[i]);
            }
        } else {
            for (std::size_t i = 0; i != n; ++i) {
                if (array.IsValid(i)) {
                    out_[i] = epoch + T(v[i]);
                }
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    ::boost::posix_time::ptime *out_;
};

} // namespace internal

} // namespace dataframe

#endif // DATAFRAME_INTERNAL_DATE_TIME_VISITOR_HPP
