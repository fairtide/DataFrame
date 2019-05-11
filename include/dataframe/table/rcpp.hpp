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

#ifndef DATAFRAME_RCPP_HPP
#define DATAFRAME_RCPP_HPP

#include <Rcpp.h>
#undef Free

#include <dataframe/table/data_frame.hpp>

namespace dataframe {

namespace internal {

template <typename T>
class RcppPrimitiveVisitor : public ::arrow::ArrayVisitor
{
  public:
    RcppPrimitiveVisitor(T *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::BooleanArray &array) final
    {
        auto n = array.length();
        *out_ = T(::Rcpp::no_init(static_cast<int>(n)));
        auto &values = *out_;
        for (std::int64_t i = 0; i != n; ++i) {
            values[static_cast<int>(i)] = array.Value(i);
        }

        return ::arrow::Status::OK();
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
        auto n = array.length();
        auto v = array.raw_values();
        *out_ = T(::Rcpp::no_init(static_cast<int>(n)));
        std::copy_n(v, n, out_->begin());

        return ::arrow::Status::OK();
    }

  private:
    T *out_;
};

class RcppCharacterVisitor final : public ::arrow::ArrayVisitor
{
  public:
    RcppCharacterVisitor(::Rcpp::CharacterVector *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        auto n = array.length();
        *out_ = ::Rcpp::CharacterVector(::Rcpp::no_init(static_cast<int>(n)));
        auto &values = *out_;
        for (std::int64_t i = 0; i != n; ++i) {
            if (array.IsValid(i)) {
                values[i] = array.GetString(i);
            } else {
                values[i] = NA_STRING;
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    ::Rcpp::CharacterVector *out_;
};

class RcppDateVisitor final : public ::arrow::ArrayVisitor
{
  public:
    RcppDateVisitor(::Rcpp::newDateVector *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        auto n = array.length();
        auto v = array.raw_values();
        *out_ = ::Rcpp::newDateVector(static_cast<int>(n));
        std::copy_n(v, n, out_->begin());

        return ::arrow::Status::OK();
    }

  private:
    ::Rcpp::newDateVector *out_;
};

class RcppDatetimeVisitor final : public ::arrow::ArrayVisitor
{
  public:
    RcppDatetimeVisitor(::Rcpp::newDatetimeVector *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        auto type =
            std::static_pointer_cast<::arrow::TimestampType>(array.type());

        std::string timezone("UTC");
        if (!type->timezone().empty()) {
            timezone = type->timezone();
        }

        *out_ = ::Rcpp::newDatetimeVector(
            static_cast<int>(array.length()), timezone.c_str());

        auto n = out_->size();
        auto v = out_->begin();
        std::copy_n(array.raw_values(), n, v);

        double unit = 1;
        switch (type->unit()) {
            case ::arrow::TimeUnit::SECOND:
                unit = 1;
                break;
            case ::arrow::TimeUnit::MILLI:
                unit = 1e-3;
                break;
            case ::arrow::TimeUnit::MICRO:
                unit = 1e-6;
                break;
            case ::arrow::TimeUnit::NANO:
                unit = 1e-9;
                break;
        }

        if (unit < 1) {
            for (auto i = 0; i != n; ++i) {
                v[i] *= unit;
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    ::Rcpp::newDatetimeVector *out_;
};

template <typename T, typename V>
inline void r_set_na(const ::arrow::Array &values, T na, V &v)
{
    if (values.null_count() == 0) {
        return;
    }

    for (auto i = 0; i != v.size(); ++i) {
        if (values.IsNull(i)) {
            v[i] = na;
        }
    }
}

inline std::vector<std::string_view> to_string_view(
    const ::Rcpp::CharacterVector &v)
{
    std::vector<std::string_view> ret;
    ret.reserve(static_cast<std::size_t>(v.size()));
    for (auto i = 0; i != v.size(); ++i) {
        auto str = v[i].get();
        ret.emplace_back(CHAR(str), static_cast<std::size_t>(LENGTH(str)));
    }

    return ret;
}

} // namespace internal

inline std::shared_ptr<::arrow::Array> make_array(
    const ::Rcpp::LogicalVector &values)
{
    ::arrow::BooleanBuilder builder(::arrow::default_memory_pool());

    DF_ARROW_ERROR_HANDLER(builder.Reserve(values.size()));

    for (auto &&v : values) {
        if (v == NA_LOGICAL) {
            DF_ARROW_ERROR_HANDLER(builder.AppendNull());
        } else {
            DF_ARROW_ERROR_HANDLER(builder.Append(static_cast<bool>(v)));
        }
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

inline std::shared_ptr<::arrow::Array> make_array(
    const ::Rcpp::IntegerVector &values)
{
    // if (values.hasAttribute("class")) {
    //     ::Rcpp::CharacterVector cls = values.attr("class");
    //     std::string clsname(cls.at(0));

    //     if (clsname == "ordered" || clsname == "factor") {
    //         if (!values.hasAttribute("levels")) {
    //             throw DataFrameException("Attribute levels not found");
    //         }

    //         ::Rcpp::CharacterVector levels = values.attr("levels");

    //         auto lvls = internal::to_string_view(levels);

    //         CategoricalArray ret;
    //         ret.reserve(static_cast<std::size_t>(values.size()));
    //         for (auto i : values) {
    //             if (i == NA_INTEGER) {
    //                 ret.emplace_back();
    //             } else {
    //                 ret.emplace_back(lvls.at(static_cast<std::size_t>(i -
    //                 1)));
    //             }
    //         }

    //         return make_array(ret);
    //     }
    // }

    std::vector<bool> mask;
    for (auto i = 0; i != values.size(); ++i) {
        mask.push_back(values[i] != NA_INTEGER);
    }

    return make_array<int>(values.begin(), values.end(), mask.begin());
}

inline std::shared_ptr<::arrow::Array> make_array(
    const ::Rcpp::NumericVector &values)
{
    std::vector<bool> mask;
    for (auto i = 0; i != values.size(); ++i) {
        auto v = values[i];
        mask.push_back(!(R_IsNA(v) && !R_IsNaN(v)));
    }

    if (values.hasAttribute("class")) {
        ::Rcpp::CharacterVector cls = values.attr("class");
        std::string clsname(cls.at(0));

        if (clsname == "Date") {
            return make_array<Datestamp<DateUnit::Day>>(
                values.begin(), values.end(), mask.begin());
        }

        if (clsname == "POSIXct") {
            auto n = static_cast<std::size_t>(values.size());
            auto v = values.begin();
            std::vector<double> t(n);
            for (std::size_t i = 0; i != n; ++i) {
                t[i] = v[i] * 1e6;
            }

            return make_array<Timestamp<TimeUnit::Microsecond>>(
                t.begin(), t.end(), mask.begin());
        }
    }

    return make_array<double>(values.begin(), values.end(), mask.begin());
}

inline std::shared_ptr<::arrow::Array> make_array(
    const ::Rcpp::CharacterVector &values)
{
    return make_array<std::string>(internal::to_string_view(values));
}

inline DataFrame make_dataframe(const ::Rcpp::List &list)
{
    DataFrame ret;

    ::Rcpp::CharacterVector names = list.names();

    for (auto i = 0; i != list.size(); ++i) {
        SEXP v = list.at(static_cast<std::size_t>(i));
        auto type = TYPEOF(v);
        std::string key(names.at(static_cast<std::size_t>(i)));
        switch (type) {
            case INTSXP:
                ret[key] = make_array(::Rcpp::as<::Rcpp::IntegerVector>(v));
                break;
            case REALSXP:
                ret[key] = make_array(::Rcpp::as<::Rcpp::NumericVector>(v));
                break;
            case LGLSXP:
                ret[key] = make_array(::Rcpp::as<::Rcpp::LogicalVector>(v));
                break;
            case STRSXP:
                ret[key] = make_array(::Rcpp::as<::Rcpp::CharacterVector>(v));
                break;
            default:
                throw DataFrameException(
                    "Unsupported SEXP type " + std::to_string(type));
        }
    }

    return ret;
}

inline void cast_array(
    const ::arrow::Array &values, ::Rcpp::LogicalVector *out)
{
    internal::RcppPrimitiveVisitor<::Rcpp::LogicalVector> visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    internal::r_set_na(values, NA_LOGICAL, *out);
}

inline void cast_array(
    const ::arrow::Array &values, ::Rcpp::IntegerVector *out)
{
    internal::RcppPrimitiveVisitor<::Rcpp::IntegerVector> visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    internal::r_set_na(values, NA_INTEGER, *out);
}

inline void cast_array(
    const ::arrow::Array &values, ::Rcpp::NumericVector *out)
{
    internal::RcppPrimitiveVisitor<::Rcpp::NumericVector> visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    internal::r_set_na(values, NA_REAL, *out);
}

inline void cast_array(
    const ::arrow::Array &values, ::Rcpp::CharacterVector *out)
{
    internal::RcppCharacterVisitor visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    internal::r_set_na(values, NA_STRING, *out);
}

inline void cast_array(
    const ::arrow::Array &values, ::Rcpp::newDateVector *out)
{
    internal::RcppDateVisitor visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    internal::r_set_na(values, NA_REAL, *out);
}

inline void cast_array(
    const ::arrow::Array &values, ::Rcpp::newDatetimeVector *out)
{
    internal::RcppDatetimeVisitor visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
    internal::r_set_na(values, NA_REAL, *out);
}

inline void cast_dataframe(const DataFrame &df, ::Rcpp::List *out)
{
    ::Rcpp::List ret;

    auto ncol = df.ncol();
    for (std::size_t i = 0; i != ncol; ++i) {
        auto col = df[i];
        auto key = col.name();

        if (col.is_type<bool>()) {
            ::Rcpp::LogicalVector vec;
            cast_array(*col.data(), &vec);
            ret[key] = vec;
        } else if (col.is_real() || col.is_type<std::int64_t>() ||
            col.is_type<std::uint64_t>()) {
            ::Rcpp::NumericVector vec;
            cast_array(*col.data(), &vec);
            ret[key] = vec;
        } else if (col.is_integer()) {
            ::Rcpp::IntegerVector vec;
            cast_array(*col.data(), &vec);
            ret[key] = vec;
        } else if (col.is_binary()) {
            ::Rcpp::CharacterVector vec;
            cast_array(*col.data(), &vec);
            ret[key] = vec;
        } else if (col.is_type<Datestamp<DateUnit::Day>>()) {
            ::Rcpp::newDateVector vec(0);
            cast_array(*col.data(), &vec);
            ret[key] = vec;
        } else if (col.is_timestamp()) {
            ::Rcpp::newDatetimeVector vec(0);
            cast_array(*col.data(), &vec);
            ret[key] = vec;
            // } else if (col.is_dictionary()) {
            //     auto data = col.as_view<CategoricalArray>();

            //     auto &lvl = data.levels();
            //     ::Rcpp::CharacterVector levels;
            //     for (auto &&v : lvl) {
            //         levels.push_back(v);
            //     }

            //     auto &idx = data.index();
            //     ::Rcpp::IntegerVector index;
            //     for (auto &&v : idx) {
            //         if (v < 0) {
            //             index.push_back(NA_INTEGER);
            //         } else {
            //             index.push_back(v + 1);
            //         }
            //     }

            //     index.attr("class") = "factor";
            //     index.attr("levels") = levels;

            //     ret[key] = index;
        } else {
            throw DataFrameException(
                "Unknown dtype cannot be converted to JSON");
        }
    }

    *out = ret;
}

} // namespace dataframe

#endif // DATAFRAME_RCPP_HPP
