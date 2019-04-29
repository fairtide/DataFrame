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

#ifndef DATAFRAME_SERIALIZER_BSON_COMPRESS_HPP
#define DATAFRAME_SERIALIZER_BSON_COMPRESS_HPP

#include <dataframe/serializer/bson/schema.hpp>
#include <arrow/api.h>
#include <bsoncxx/types.hpp>
#include <lz4.h>
#include <lz4hc.h>

namespace dataframe {

namespace bson {

inline ::bsoncxx::types::b_binary compress(const ::arrow::Buffer &buffer,
    Vector<std::uint8_t> *out, int compression_level)
{
    auto n = static_cast<int>(buffer.size());
    auto src = reinterpret_cast<const char *>(buffer.data());

    auto m = ::LZ4_compressBound(n);
    out->resize(static_cast<std::size_t>(m) + sizeof(std::int32_t));

    auto dst = reinterpret_cast<char *>(out->data());
    *reinterpret_cast<std::int32_t *>(dst) = n;
    dst += sizeof(std::int32_t);

    auto len = compression_level <= 0 ?
        ::LZ4_compress_default(src, dst, n, m) :
        ::LZ4_compress_HC(src, dst, n, m, compression_level);

    out->resize(static_cast<std::size_t>(len) + sizeof(std::int32_t));

    ::bsoncxx::types::b_binary ret;
    ret.sub_type = ::bsoncxx::binary_sub_type::k_binary;
    ret.bytes = out->data();
    ret.size = static_cast<std::uint32_t>(out->size());

    return ret;
}

template <typename T>
inline ::bsoncxx::types::b_binary compress(std::int64_t n, const T *data,
    Vector<std::uint8_t> *out, int compression_level)
{
    return compress(
        ::arrow::Buffer(reinterpret_cast<const std::uint8_t *>(data),
            n * static_cast<std::int64_t>(sizeof(T))),
        out, compression_level);
}

inline std::unique_ptr<::arrow::Buffer> decompress(
    const ::bsoncxx::types::b_binary &bin, ::arrow::MemoryPool *pool)
{
    auto n = static_cast<int>(bin.size);
    auto src = reinterpret_cast<const char *>(bin.bytes);

    auto m = *reinterpret_cast<const std::int32_t *>(src);
    n -= sizeof(std::int32_t);
    src += sizeof(std::int32_t);

    std::unique_ptr<::arrow::Buffer> ret;
    DF_ARROW_ERROR_HANDLER(::arrow::AllocateBuffer(pool, m, &ret));
    auto &buf = dynamic_cast<::arrow::MutableBuffer &>(*ret);
    auto dst = reinterpret_cast<char *>(buf.mutable_data());

    auto k = ::LZ4_decompress_safe(src, dst, n, m);

    if (k != m) {
        throw DataFrameException("Decompress failed");
    }

    return ret;
}

template <typename T>
inline std::unique_ptr<::arrow::Buffer> decompress(
    const ::bsoncxx::types::b_binary &bin, ::arrow::MemoryPool *pool)
{
    auto buffer = decompress(bin, pool);

    if (buffer->size() % static_cast<std::int64_t>(sizeof(T)) != 0) {
        throw DataFrameException("Incorrect buffer size " +
            std::to_string(buffer->size()) + " byte width " +
            std::to_string(sizeof(T)));
    }

    return buffer;
}

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_COMPRESS_HPP
