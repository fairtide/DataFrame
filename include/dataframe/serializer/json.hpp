//============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//============================================================================

#ifndef DATAFRAME_SERIALIZER_JSON_HPP
#define DATAFRAME_SERIALIZER_JSON_HPP

#include <dataframe/serializer/base.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace dataframe {

class JSONRowWriter : public Writer
{
    struct Visitor : ::arrow::ArrayVisitor {
        ::rapidjson::GenericStringRef<char> key;
        std::vector<::rapidjson::Document> &rows;

        Visitor(const std::string &k, std::vector<::rapidjson::Document> &r)
            : key(k.data(), static_cast<unsigned>(k.size()))
            , rows(r)
        {
        }

        ::arrow::Status Visit(const ::arrow::NullArray &) override
        {
            for (auto &doc : rows) {
                doc.AddMember(key, ::rapidjson::Value(), doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::BooleanArray &array) override
        {
            std::int64_t j = 0;
            for (auto &doc : rows) {
                doc.AddMember(key, array.GetView(j++), doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Array &array) override        \
    {                                                                         \
        auto p = array.raw_values();                                          \
        for (auto &doc : rows) {                                              \
            doc.AddMember(key, *p++, doc.GetAllocator());                     \
        }                                                                     \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

        DF_DEFINE_VISITOR(Int8)
        DF_DEFINE_VISITOR(Int16)
        DF_DEFINE_VISITOR(Int32)
        DF_DEFINE_VISITOR(Int64)
        DF_DEFINE_VISITOR(UInt8)
        DF_DEFINE_VISITOR(UInt16)
        DF_DEFINE_VISITOR(UInt32)
        DF_DEFINE_VISITOR(UInt64)
        // DF_DEFINE_VISITOR(HalfFloat)
        DF_DEFINE_VISITOR(Float)
        DF_DEFINE_VISITOR(Double)

#undef DF_DEFINE_VISITOR

        ::arrow::Status Visit(const ::arrow::Date32Array &array) override
        {
            ::boost::gregorian::date epoch(1970, 1, 1);

            auto p = array.raw_values();

            for (auto &doc : rows) {
                auto v = epoch + ::boost::gregorian::days(*p++);
                doc.AddMember(key,
                    ::rapidjson::Value().SetString(
                        ::boost::gregorian::to_iso_extended_string(v),
                        doc.GetAllocator()),
                    doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::Date64Array &array) override
        {
            ::boost::gregorian::date epoch_date(1970, 1, 1);
            ::boost::posix_time::ptime epoch(epoch_date);

            auto p = array.raw_values();

            for (auto &doc : rows) {
                auto v = epoch + ::boost::posix_time::milliseconds(*p++);
                doc.AddMember(key,
                    ::rapidjson::Value().SetString(
                        ::boost::posix_time::to_iso_extended_string(v) + "Z",
                        doc.GetAllocator()),
                    doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
        {
            ::boost::gregorian::date epoch_date(1970, 1, 1);
            ::boost::posix_time::ptime epoch(epoch_date);

            auto p = array.raw_values();
            auto u = static_cast<const ::arrow::TimestampType &>(*array.type())
                         .unit();

            for (auto &doc : rows) {
                auto v = epoch;

                switch (u) {
                    case ::arrow::TimeUnit::SECOND:
                        v += ::boost::posix_time::seconds(*p++);
                        break;
                    case ::arrow::TimeUnit::MILLI:
                        v += ::boost::posix_time::milliseconds(*p++);
                        break;
                    case ::arrow::TimeUnit::MICRO:
                        v += ::boost::posix_time::microseconds(*p++);
                        break;
                    case ::arrow::TimeUnit::NANO:
#ifdef BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                        v += ::boost::posix_time::nanoseconds(*p++);
#else
                        v += ::boost::posix_time::microseconds(*p++ / 1000);
#endif
                        break;
                }

                doc.AddMember(key,
                    ::rapidjson::Value().SetString(
                        ::boost::posix_time::to_iso_extended_string(v) + "Z",
                        doc.GetAllocator()),
                    doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::Time32Array &array) override
        {
            auto p = array.raw_values();
            auto u =
                static_cast<const ::arrow::Time32Type &>(*array.type()).unit();

            for (auto &doc : rows) {
                ::boost::posix_time::time_duration v;

                switch (u) {
                    case ::arrow::TimeUnit::SECOND:
                        v = ::boost::posix_time::seconds(*p++);
                        break;
                    case ::arrow::TimeUnit::MILLI:
                        v = ::boost::posix_time::milliseconds(*p++);
                        break;
                    case ::arrow::TimeUnit::MICRO:
                        throw DataFrameException("Unexpected unit for Time32");
                    case ::arrow::TimeUnit::NANO:
                        throw DataFrameException("Unexpected unit for Time32");
                }

                doc.AddMember(key,
                    ::rapidjson::Value().SetString(
                        ::boost::posix_time::to_simple_string(v),
                        doc.GetAllocator()),
                    doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::Time64Array &array) override
        {
            auto p = array.raw_values();
            auto u =
                static_cast<const ::arrow::Time32Type &>(*array.type()).unit();

            for (auto &doc : rows) {
                ::boost::posix_time::time_duration v;

                switch (u) {
                    case ::arrow::TimeUnit::SECOND:
                        throw DataFrameException("Unexpected unit for Time32");
                    case ::arrow::TimeUnit::MILLI:
                        throw DataFrameException("Unexpected unit for Time32");
                    case ::arrow::TimeUnit::MICRO:
                        v = ::boost::posix_time::microseconds(*p++);
                        break;
                    case ::arrow::TimeUnit::NANO:
#ifdef BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                        v = ::boost::posix_time::nanoseconds(*p++);
#else
                        v = ::boost::posix_time::microseconds(*p++ / 1000);
#endif
                        break;
                }

                doc.AddMember(key,
                    ::rapidjson::Value().SetString(
                        ::boost::posix_time::to_simple_string(v),
                        doc.GetAllocator()),
                    doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        // DF_DEFINE_VISITOR(Interval)
        // DF_DEFINE_VISITOR(FixedSizeBinary)
        // DF_DEFINE_VISITOR(Decimal128)
        // DF_DEFINE_VISITOR(Binary)

        ::arrow::Status Visit(const ::arrow::StringArray &array) override
        {
            std::int64_t j = 0;
            for (auto &doc : rows) {
                auto v = array.GetView(j++);
                doc.AddMember(key,
                    ::rapidjson::Value().SetString(v.data(),
                        static_cast<unsigned>(v.size()), doc.GetAllocator()),
                    doc.GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        // DF_DEFINE_VISITOR(List)
        // DF_DEFINE_VISITOR(Struct)
        // DF_DEFINE_VISITOR(Union)
        // DF_DEFINE_VISITOR(Dictionary) // TODO
    };

  public:
    JSONRowWriter(std::string root)
        : root_(std::move(root))
    {
        if (root_.empty()) {
            throw DataFrameException(
                "root key cannot be empty for JSONRowWriter");
        }
    }

    std::size_t size() const override { return buffer_.GetSize(); }

    const std::uint8_t *data() const override
    {
        return reinterpret_cast<const std::uint8_t *>(buffer_.GetString());
    }

    void write(const DataFrame &df) override
    {
        auto nrow = df.nrow();
        auto ncol = df.ncol();

        ::rapidjson::Document root;
        root.SetObject();

        std::vector<::rapidjson::Document> rows;
        for (std::size_t i = 0; i != nrow; ++i) {
            rows.emplace_back(::rapidjson::kObjectType, &root.GetAllocator());
        }

        std::vector<std::string> keys;
        for (std::size_t i = 0; i != ncol; ++i) {
            keys.push_back(df[i].name());
        }

        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];
            Visitor visitor(keys[i], rows);
            DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));
        }

        ::rapidjson::Value data(::rapidjson::kArrayType);
        for (auto &r : rows) {
            data.PushBack(std::move(r), root.GetAllocator());
        }

        root.AddMember(::rapidjson::StringRef(root_.data(), root_.size()),
            data, root.GetAllocator());

        ::rapidjson::Writer<::rapidjson::StringBuffer> visitor(buffer_);
        root.Accept(visitor);
    }

  private:
    std::string root_;
    ::rapidjson::StringBuffer buffer_;
};

class JSONColumnWriter : public Writer
{
    struct Visitor : public ::arrow::ArrayVisitor {
        ::rapidjson::Document *root;
        ::rapidjson::Value *value;

        ::arrow::Status Visit(const ::arrow::NullArray &array) override
        {
            auto n = array.length();
            for (std::int64_t i = 0; i != n; ++i) {
                value->PushBack(::rapidjson::Value(), root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::BooleanArray &array) override
        {
            auto n = array.length();
            for (std::int64_t i = 0; i != n; ++i) {
                value->PushBack(array.GetView(i), root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Array &array) override        \
    {                                                                         \
        auto n = array.length();                                              \
        auto p = array.raw_values();                                          \
        for (std::int64_t i = 0; i != n; ++i) {                               \
            value->PushBack(p[i], root->GetAllocator());                      \
        }                                                                     \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

        DF_DEFINE_VISITOR(Int8)
        DF_DEFINE_VISITOR(Int16)
        DF_DEFINE_VISITOR(Int32)
        DF_DEFINE_VISITOR(Int64)
        DF_DEFINE_VISITOR(UInt8)
        DF_DEFINE_VISITOR(UInt16)
        DF_DEFINE_VISITOR(UInt32)
        DF_DEFINE_VISITOR(UInt64)
        // DF_DEFINE_VISITOR(HalfFloat)
        DF_DEFINE_VISITOR(Float)
        DF_DEFINE_VISITOR(Double)

#undef DF_DEFINE_VISITOR

        ::arrow::Status Visit(const ::arrow::Date32Array &array) override
        {
            ::boost::gregorian::date epoch(1970, 1, 1);

            auto n = array.length();
            auto p = array.raw_values();

            for (std::int64_t i = 0; i != n; ++i) {
                auto v = epoch + ::boost::gregorian::days(p[i]);
                value->PushBack(
                    ::rapidjson::Value().SetString(
                        ::boost::gregorian::to_iso_extended_string(v),
                        root->GetAllocator()),
                    root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::Date64Array &array) override
        {
            ::boost::gregorian::date epoch_date(1970, 1, 1);
            ::boost::posix_time::ptime epoch(epoch_date);

            auto n = array.length();
            auto p = array.raw_values();

            for (std::int64_t i = 0; i != n; ++i) {
                auto v = epoch + ::boost::posix_time::milliseconds(p[i]);
                value->PushBack(
                    ::rapidjson::Value().SetString(
                        ::boost::posix_time::to_iso_extended_string(v) + "Z",
                        root->GetAllocator()),
                    root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
        {
            ::boost::gregorian::date epoch_date(1970, 1, 1);
            ::boost::posix_time::ptime epoch(epoch_date);

            auto n = array.length();
            auto p = array.raw_values();
            auto u = static_cast<const ::arrow::TimestampType &>(*array.type())
                         .unit();

            for (std::int64_t i = 0; i != n; ++i) {
                auto v = epoch;

                switch (u) {
                    case ::arrow::TimeUnit::SECOND:
                        v += ::boost::posix_time::seconds(p[i]);
                        break;
                    case ::arrow::TimeUnit::MILLI:
                        v += ::boost::posix_time::milliseconds(p[i]);
                        break;
                    case ::arrow::TimeUnit::MICRO:
                        v += ::boost::posix_time::microseconds(p[i]);
                        break;
                    case ::arrow::TimeUnit::NANO:
#ifdef BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                        v += ::boost::posix_time::nanoseconds(p[i]);
#else
                        v += ::boost::posix_time::microseconds(p[i] / 1000);
#endif
                        break;
                }

                value->PushBack(
                    ::rapidjson::Value().SetString(
                        ::boost::posix_time::to_iso_extended_string(v) + "Z",
                        root->GetAllocator()),
                    root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::Time32Array &array) override
        {
            auto n = array.length();
            auto p = array.raw_values();
            auto u =
                static_cast<const ::arrow::Time32Type &>(*array.type()).unit();

            for (std::int64_t i = 0; i != n; ++i) {
                ::boost::posix_time::time_duration v;

                switch (u) {
                    case ::arrow::TimeUnit::SECOND:
                        v = ::boost::posix_time::seconds(p[i]);
                        break;
                    case ::arrow::TimeUnit::MILLI:
                        v = ::boost::posix_time::milliseconds(p[i]);
                        break;
                    case ::arrow::TimeUnit::MICRO:
                        throw DataFrameException("Unexpected unit for Time32");
                    case ::arrow::TimeUnit::NANO:
                        throw DataFrameException("Unexpected unit for Time32");
                }

                value->PushBack(::rapidjson::Value().SetString(
                                    ::boost::posix_time::to_simple_string(v),
                                    root->GetAllocator()),
                    root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        ::arrow::Status Visit(const ::arrow::Time64Array &array) override
        {
            auto n = array.length();
            auto p = array.raw_values();
            auto u =
                static_cast<const ::arrow::Time32Type &>(*array.type()).unit();

            for (std::int64_t i = 0; i != n; ++i) {
                ::boost::posix_time::time_duration v;

                switch (u) {
                    case ::arrow::TimeUnit::SECOND:
                        throw DataFrameException("Unexpected unit for Time32");
                    case ::arrow::TimeUnit::MILLI:
                        throw DataFrameException("Unexpected unit for Time32");
                    case ::arrow::TimeUnit::MICRO:
                        v = ::boost::posix_time::microseconds(p[i]);
                        break;
                    case ::arrow::TimeUnit::NANO:
#ifdef BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                        v = ::boost::posix_time::nanoseconds(p[i]);
#else
                        v = ::boost::posix_time::microseconds(p[i] / 1000);
#endif
                        break;
                }

                value->PushBack(::rapidjson::Value().SetString(
                                    ::boost::posix_time::to_simple_string(v),
                                    root->GetAllocator()),
                    root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        // DF_DEFINE_VISITOR(Interval)
        // DF_DEFINE_VISITOR(FixedSizeBinary)
        // DF_DEFINE_VISITOR(Decimal128)
        // DF_DEFINE_VISITOR(Binary)

        ::arrow::Status Visit(const ::arrow::StringArray &array) override
        {
            auto n = array.length();
            for (std::int64_t i = 0; i != n; ++i) {
                auto v = array.GetView(i);
                value->PushBack(
                    ::rapidjson::Value().SetString(v.data(),
                        static_cast<unsigned>(v.size()), root->GetAllocator()),
                    root->GetAllocator());
            }

            return ::arrow::Status::OK();
        }

        // DF_DEFINE_VISITOR(List)
        // DF_DEFINE_VISITOR(Struct)
        // DF_DEFINE_VISITOR(Union)
        // DF_DEFINE_VISITOR(Dictionary) // TODO
    };

  public:
    std::size_t size() const override { return buffer_.GetSize(); }

    const std::uint8_t *data() const override
    {
        return reinterpret_cast<const std::uint8_t *>(buffer_.GetString());
    }

    void write(const DataFrame &df) override
    {
        auto ncol = df.ncol();

        ::rapidjson::Document root(::rapidjson::kObjectType);

        std::vector<std::string> keys;
        for (std::size_t i = 0; i != ncol; ++i) {
            keys.push_back(df[i].name());
        }

        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];
            auto key = ::rapidjson::StringRef(keys[i].data(), keys[i].size());
            ::rapidjson::Value value(::rapidjson::kArrayType);
            Visitor visitor;
            visitor.root = &root;
            visitor.value = &value;
            DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));
            root.AddMember(key, value, root.GetAllocator());
        }

        ::rapidjson::Writer<::rapidjson::StringBuffer> visitor(buffer_);
        root.Accept(visitor);
    }

  private:
    ::rapidjson::StringBuffer buffer_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_JSON_HPP
