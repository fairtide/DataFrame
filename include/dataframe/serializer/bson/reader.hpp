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

#ifndef DATAFRAME_SERIALIZER_BSON_READER_HPP
#define DATAFRAME_SERIALIZER_BSON_READER_HPP

#include <dataframe/serializer/base.hpp>
#include <dataframe/serializer/bson/compress.hpp>
#include <dataframe/serializer/bson/schema.hpp>

namespace dataframe {

namespace bson {

class ColumnReader : public ::arrow::TypeVisitor
{
  public:
    ColumnReader(::arrow::MemoryPool *pool)
        : pool_(pool)
    {
    }

    std::shared_ptr<::arrow::Array> read(const ::bsoncxx::document::view &view)
    {
        view_ = view;
        data_ = std::make_shared<::arrow::ArrayData>(nullptr, 0);

        data_->type = make_type(view);
        data_->buffers.reserve(2);
        data_->buffers.push_back(make_mask(view[Schema::MASK()].get_binary()));

        DF_ARROW_ERROR_HANDLER(data_->type->Accept(this));

        return ::arrow::MakeArray(std::move(data_));
    }

  private:
#define DF_DEFINE_VISITOR(TypeName)                                           \
    ::arrow::Status Visit(const ::arrow::TypeName##Type &type) final          \
    {                                                                         \
        return make_data(*data_, view_, type);                                \
    }

    DF_DEFINE_VISITOR(Null)
    // DF_DEFINE_VISITOR(Boolean)
    DF_DEFINE_VISITOR(Int8)
    DF_DEFINE_VISITOR(Int16)
    DF_DEFINE_VISITOR(Int32)
    DF_DEFINE_VISITOR(Int64)
    DF_DEFINE_VISITOR(UInt8)
    DF_DEFINE_VISITOR(UInt16)
    DF_DEFINE_VISITOR(UInt32)
    DF_DEFINE_VISITOR(UInt64)
    DF_DEFINE_VISITOR(HalfFloat)
    DF_DEFINE_VISITOR(Float)
    DF_DEFINE_VISITOR(Double)
    DF_DEFINE_VISITOR(Date32)
    DF_DEFINE_VISITOR(Date64)
    DF_DEFINE_VISITOR(Timestamp)
    DF_DEFINE_VISITOR(Time32)
    DF_DEFINE_VISITOR(Time64)
    DF_DEFINE_VISITOR(Interval)
    // DF_DEFINE_VISITOR(Decimal128)
    DF_DEFINE_VISITOR(FixedSizeBinary)
    DF_DEFINE_VISITOR(Binary)
    DF_DEFINE_VISITOR(String)
    // DF_DEFINE_VISITOR(List)
    // DF_DEFINE_VISITOR(Struct)
    // DF_DEFINE_VISITOR(Union)
    // DF_DEFINE_VISITOR(Dictionary)

#undef DF_DEFINE_VISITOR

    std::shared_ptr<::arrow::Buffer> make_mask(
        const ::bsoncxx::types::b_binary &)
    {
        return nullptr;
    }

    ::arrow::Status make_data(::arrow::ArrayData &data,
        const ::bsoncxx::document::view &view, const ::arrow::NullType &)
    {
        data.length = view[Schema::DATA()].get_int64().value;

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_MAKE_DATA(TypeClass)                                        \
    ::arrow::Status make_data(::arrow::ArrayData &data,                       \
        const ::bsoncxx::document::view &view,                                \
        const ::arrow::TypeClass##Type &)                                     \
    {                                                                         \
        using T = typename ::arrow::TypeClass##Type::c_type;                  \
                                                                              \
        auto buffer =                                                         \
            decompress<T>(view[Schema::DATA()].get_binary(), pool_);          \
                                                                              \
        data.length = buffer->size() / static_cast<std::int64_t>(sizeof(T));  \
        data.buffers.push_back(std::move(buffer));                            \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_MAKE_DATA(Int8)
    DF_DEFINE_MAKE_DATA(Int16)
    DF_DEFINE_MAKE_DATA(Int32)
    DF_DEFINE_MAKE_DATA(Int64)
    DF_DEFINE_MAKE_DATA(UInt8)
    DF_DEFINE_MAKE_DATA(UInt16)
    DF_DEFINE_MAKE_DATA(UInt32)
    DF_DEFINE_MAKE_DATA(UInt64)
    DF_DEFINE_MAKE_DATA(HalfFloat)
    DF_DEFINE_MAKE_DATA(Float)
    DF_DEFINE_MAKE_DATA(Double)
    DF_DEFINE_MAKE_DATA(Time32)
    DF_DEFINE_MAKE_DATA(Time64)
    DF_DEFINE_MAKE_DATA(Interval)

#undef DF_DEFINE_MAKE_DATA

#define DF_DEFINE_MAKE_DATA(TypeClass)                                        \
    ::arrow::Status make_data(::arrow::ArrayData &data,                       \
        const ::bsoncxx::document::view &view,                                \
        const ::arrow::TypeClass##Type &)                                     \
    {                                                                         \
        using T = typename ::arrow::TypeClass##Type::c_type;                  \
                                                                              \
        auto buffer =                                                         \
            decompress<T>(view[Schema::DATA()].get_binary(), pool_);          \
                                                                              \
        auto n = buffer->size() / static_cast<std::int64_t>(sizeof(T));       \
        auto p = reinterpret_cast<T *>(                                       \
            dynamic_cast<::arrow::MutableBuffer &>(*buffer).mutable_data());  \
                                                                              \
        for (std::int64_t i = 1; i < n; ++i) {                                \
            p[i] += p[i - 1];                                                 \
        }                                                                     \
                                                                              \
        data.length = n;                                                      \
        data.buffers.push_back(std::move(buffer));                            \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_MAKE_DATA(Date32)
    DF_DEFINE_MAKE_DATA(Date64)
    DF_DEFINE_MAKE_DATA(Timestamp)

    ::arrow::Status make_data(::arrow::ArrayData &data,
        const ::bsoncxx::document::view &view,
        const ::arrow::FixedSizeBinaryType &type)
    {
        auto buffer = decompress(view[Schema::DATA()].get_binary(), pool_);

        if (buffer->size() % type.byte_width() != 0) {
            throw DataFrameException("Incorrect buffer size for POD type");
        }

        data.length = buffer->size() / type.byte_width();
        data.buffers.push_back(std::move(buffer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status make_data(::arrow::ArrayData &data,
        const ::bsoncxx::document::view &view, const ::arrow::BinaryType &)
    {
        auto values = decompress(view[Schema::DATA()].get_binary(), pool_);

        auto offsets = decompress<std::int32_t>(
            view[Schema::OFFSET()].get_binary(), pool_);

        auto byte_width = static_cast<std::int64_t>(sizeof(std::int32_t));
        data.length = offsets->size() / byte_width - 1;
        data.buffers.push_back(std::move(offsets));
        data.buffers.push_back(std::move(values));

        return ::arrow::Status::OK();
    }

#undef DF_DEFINE_MAKE_DATA

    static auto make_type_mapping()
    {
        std::map<std::string, std::shared_ptr<::arrow::DataType>> ret;

        ret.emplace("null", ::arrow::null());
        ret.emplace("int8", ::arrow::int8());
        ret.emplace("int16", ::arrow::int16());
        ret.emplace("int32", ::arrow::int32());
        ret.emplace("int64", ::arrow::int64());
        ret.emplace("uint8", ::arrow::uint8());
        ret.emplace("uint16", ::arrow::uint16());
        ret.emplace("uint32", ::arrow::uint32());
        ret.emplace("uint64", ::arrow::uint64());
        ret.emplace("float16", ::arrow::float16());
        ret.emplace("float32", ::arrow::float32());
        ret.emplace("float64", ::arrow::float64());
        ret.emplace("date32", ::arrow::date32());
        ret.emplace("date64", ::arrow::date64());

        ret.emplace(
            "timestamp[s]", ::arrow::timestamp(::arrow::TimeUnit::SECOND));
        ret.emplace(
            "timestamp[ms]", ::arrow::timestamp(::arrow::TimeUnit::MILLI));
        ret.emplace(
            "timestamp[us]", ::arrow::timestamp(::arrow::TimeUnit::MICRO));
        ret.emplace(
            "timestamp[ns]", ::arrow::timestamp(::arrow::TimeUnit::NANO));

        ret.emplace("time32[s]", ::arrow::time32(::arrow::TimeUnit::SECOND));
        ret.emplace("time32[ms]", ::arrow::time32(::arrow::TimeUnit::MILLI));
        ret.emplace("time32[us]", ::arrow::time32(::arrow::TimeUnit::MICRO));
        ret.emplace("time32[ns]", ::arrow::time32(::arrow::TimeUnit::NANO));

        ret.emplace("time64[s]", ::arrow::time64(::arrow::TimeUnit::SECOND));
        ret.emplace("time64[ms]", ::arrow::time64(::arrow::TimeUnit::MILLI));
        ret.emplace("time64[us]", ::arrow::time64(::arrow::TimeUnit::MICRO));
        ret.emplace("time64[ns]", ::arrow::time64(::arrow::TimeUnit::NANO));

        ret.emplace("interval[ym]",
            std::make_shared<::arrow::IntervalType>(
                ::arrow::IntervalType::Unit::YEAR_MONTH));
        ret.emplace("interval[dt]",
            std::make_shared<::arrow::IntervalType>(
                ::arrow::IntervalType::Unit::DAY_TIME));

        ret.emplace("utf8", ::arrow::utf8());
        ret.emplace("bytes", ::arrow::binary());

        return ret;
    }

    static const auto &get_type_mapping()
    {
        static auto ret = make_type_mapping();

        return ret;
    }

    std::shared_ptr<::arrow::DataType> make_type(
        const ::bsoncxx::document::view &view)
    {
        auto type = view[Schema::TYPE()].get_utf8().value;
        std::string stype(type.data(), type.size());

        auto &mapping = get_type_mapping();
        auto iter = mapping.find(stype);
        if (iter != mapping.end()) {
            return iter->second;
        }

        auto tp = view_[Schema::PARAM()];

        if (stype == "decimal") {
            auto param = tp.get_document().view();
            auto precision = param["precision"].get_int32().value;
            auto scale = param["scale"].get_int32().value;
            return ::arrow::decimal(precision, scale);
        }

        if (stype == "pod") {
            return ::arrow::fixed_size_binary(tp.get_int32().value);
        }

        if (stype == "list") {
            return ::arrow::list(make_type(tp.get_document().view()));
        }

        if (stype == "struct") {
            auto param = tp.get_array().value;
            std::vector<std::shared_ptr<::arrow::Field>> fields;
            for (auto &&elem : param) {
                auto f = elem.get_document().view();
                fields.push_back(
                    ::arrow::field(f["name"].get_utf8().value.data(),
                        make_type(f["type"].get_document().view())));
            }
            return ::arrow::struct_(fields);
        }

        if (stype == "union") {
            return nullptr;
        }

        if (stype == "dictionary") {
            return nullptr;
        }

        throw DataFrameException("Unkown type " + stype);
    }

  private:
    ::arrow::MemoryPool *pool_;
    std::shared_ptr<::arrow::ArrayData> data_;
    ::bsoncxx::document::view view_;
};

} // namespace bson

class BSONReader : public Reader
{
  public:
    BSONReader(::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
        : column_reader_(pool)
    {
    }

    DataFrame read_buffer(
        std::size_t n, const std::uint8_t *buf, bool = false) override
    {
        ::bsoncxx::document::view doc(buf, n);
        ::dataframe::DataFrame data;

        for (auto &&col : doc) {
            auto b_key = col.key();
            std::string key(b_key.data(), b_key.size());
            data[key] = column_reader_.read(col.get_document().view());
        }

        return data;
    }

  protected:
    bson::ColumnReader column_reader_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_READER_HPP
