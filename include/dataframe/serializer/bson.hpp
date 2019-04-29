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

#ifndef DATAFRAME_SERIALIZER_BSON_HPP
#define DATAFRAME_SERIALIZER_BSON_HPP

#include <dataframe/serializer/base.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <chrono>
#include <limits>

// TODO
#include <ft_ext/compression/lz4.h>

namespace dataframe {

namespace bson {

struct Schema {
    using view = ::bsoncxx::stdx::string_view;

    // column

    static view DATA() { return "d"; }
    static view MASK() { return "m"; }
    static view TYPE() { return "t"; }

    // null
    static view NULL_VALUE { return "v"; }
    static view NULL_LENGTH { return "n"; }

    // string/binary
    static view BINARY_VALUE { return "v"; }
    static view BINARY_OFFSET { return "o"; }

    // fixed size binary
    static view FIXED_SIZE_BINARY_VALUE { return "v"; }
    static view FIXED_SIZE_BINARY_WIDTH { return "n"; }

    // list
    static view LIST_VALUE { return "v"; }
    static view LIST_OFFSET { return "o"; }

    // struct
    static view STRUCT_VALUE { return "v"; }
    static view STRUCT_LABLE { return "k"; }

    // TODO dense union

    // dictionary
    static view DICTIONARY_VALUE { return "v"; }
    static view DICTIONARY_INDEX { return "i"; }
};

template <typename T>
using Vector = std::vector<T, ::arrow::stl_allocator<T>>;

using Compressor = ::ft_ext::LZ4Compressor;

inline Vector<std::uint8_t> make_mask(Compressor &compressor, std::size_t n)
{
    Vector<std::uint8_t> ret;

    ::mckl::Vector<std::uint8_t> pack_mask((n + 7) / 8);
    std::fill_n(pack_mask.data(), pack_mask.size(),
        std::numeric_limits<std::uint8_t>::max());
    if (!pack_mask.empty() && n % 8 != 0) {
        auto last = pack_mask.back();
        auto nzero = (8 - (n % 8));
        pack_mask.back() = static_cast<std::uint8_t>((last >> nzero) << nzero);
    }

    compressor.Compress(pack_mask.size(), pack_mask.data(), &ret);

    return ret;
}

template <typename Arraytype>
inline ::bsoncxx::document::value make_column(
    const ArrayType &, ColumnWriter *);

struct ColumnWriter final : public ::arrow::ArrayVisitor {
    ColumnVisitor(std::size_t nrow, int compress_level)
        : compressor(compress_level)
        , mask(make_mask(compressor, nrow))
    {
    }

    Compressor compressor;
    Vector<std::uint8_t> mask;
    Vector<std::uint8_t> buffer;
    std::unique_ptr<::bsoncxx::builder::basic::document> value;

    inline ::arrow::Status Visit(const NullArray &) final;

    inline ::arrow::Status Visit(const BooleanArray &) final;

    inline ::arrow::Status Visit(const ::arrow::Int8Array &) final;
    inline ::arrow::Status Visit(const ::arrow::Int16Array &) final;
    inline ::arrow::Status Visit(const ::arrow::Int32Array &) final;
    inline ::arrow::Status Visit(const ::arrow::Int64Array &) final;
    inline ::arrow::Status Visit(const ::arrow::UInt8Array &) final;
    inline ::arrow::Status Visit(const ::arrow::UInt16Array &) final;
    inline ::arrow::Status Visit(const ::arrow::UInt32Array &) final;
    inline ::arrow::Status Visit(const ::arrow::UInt64Array &) final;
    inline ::arrow::Status Visit(const ::arrow::HalfFloatArray &) final;
    inline ::arrow::Status Visit(const ::arrow::FloatArray &) final;
    inline ::arrow::Status Visit(const ::arrow::DoubleArray &) final;
    inline ::arrow::Status Visit(const ::arrow::Decimal128Array &) final;

    inline ::arrow::Status Visit(const ::arrow::Date32Array &) final;
    inline ::arrow::Status Visit(const ::arrow::Date64Array &) final;
    inline ::arrow::Status Visit(const ::arrow::TimestampArray &) final;

    inline ::arrow::Status Visit(const ::arrow::Time32Array &) final;
    inline ::arrow::Status Visit(const ::arrow::Time64Array &) final;
    inline ::arrow::Status Visit(const ::arrow::IntervalArray &) final;

    inline ::arrow::Status Visit(const ::arrow::StringArray &) final;
    inline ::arrow::Status Visit(const ::arrow::BinaryArray &) final;

    inline ::arrow::Status Visit(const ::arrow::FixedSizeBinaryArray &) final;

    inline ::arrow::Status Visit(const ::arrow::DictionaryArray &) final;

    inline ::arrow::Status Visit(const ::arrow::ListArray &) final;

    inline ::arrow::Status Visit(const ::arrow::StructArray &) final;

    inline ::arrow::Status Visit(const ::arrow::UnionArray &) final;

    ::bsoncxx::types::b_binary make_binary(
        std::size_t n, const std::uint8_t *bytes)
    {
        ::bsoncxx::types::b_binary ret{};
        ret.sub_type = ::bsoncxx::binary_sub_type::k_binary;
        ret.size = static_cast<std::uint32_t>(n);
        ret.bytes = bytes;

        return ret;
    }

    template <typename T>
    ::arrow::Status make_colume(
        std::size_t n, const T *values, const char *dtype)
    {
        using ::bsoncxx::builder::basic::document;
        using ::bsoncxx::builder::basic::kvp;

        compressor.Compress(n, values, &buffer);

        auto b_data = make_binary(buffer.size(), buffer.data());
        auto b_mask = make_binary(mask.size(), mask.data());

        document ret;
        ret.append(kvp(Schema::DATA(), b_data));
        ret.append(kvp(Schema::MASK(), b_mask));
        ret.append(kvp(Schema::TYPE(), dtype));

        value = std::make_unique<::bsoncxx::document::value>(ret.extract());

        return ::arrow::Status::OK();
    }
};

inline ::bsoncxx::document::value make_column(
    const ::arrow::Array &array, ColumnWriter *writer)
{
    DF_ARROW_ERROR_HANDLER(array.Accept(writer));

    return std::move(*writer->value);
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::Int8Array &array)
{
    return make_column(
        static_cast<std::size_t>(array.length()), array.raw_values(), "int8");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::UInt8Array &array)
{
    return make_column(
        static_cast<std::size_t>(array.length()), array.raw_values(), "uint8");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::int16Array &array)
{
    return make_column(
        static_cast<std::size_t>(array.length()), array.raw_values(), "int16");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::Uint16Array &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        array.raw_values(), "uint16");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::int32Array &array)
{
    return make_column(
        static_cast<std::size_t>(array.length()), array.raw_values(), "int32");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::Uint32Array &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        array.raw_values(), "uint32");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::int64Array &array)
{
    return make_column(
        static_cast<std::size_t>(array.length()), array.raw_values(), "int64");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::Uint64Array &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        array.raw_values(), "uint64");
}

inline ::arrow::Status ColumnWriter::Visit(
    const ::array::HalfFloatArray &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        array.raw_values(), "float16");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::FloatArray &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        array.raw_values(), "float32");
}

inline ::arrow::Status ColumnWriter::Visit(const ::array::DoubleArray &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        array.raw_values(), "float64");
}

inline ::arrow::Status ColumnWriter::Visit(
    const ::array::Decimal128Array &array)
{
    return make_column(static_cast<std::size_t>(array.length()),
        reinterpret_cast<const std::array<std::uint64_t, 2> *>(
            array.raw_values()),
        "decimal128");
}

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_HPP
