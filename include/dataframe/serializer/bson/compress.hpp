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

#include <arrow/allocator.h>
#include <arrow/api.h>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>
#include <lz4.h>
#include <lz4hc.h>
#include <vector>

namespace dataframe {

namespace bson {

template <typename T>
class Allocator : public ::arrow::stl_allocator<T>
{
  public:
    using ::arrow::stl_allocator<T>::stl_allocator;

    template <typename U>
    void construct(U *p)
    {
        construct_dispatch(p,
            std::integral_constant<bool,
                (std::is_scalar<U>::value || std::is_pod<U>::value)>());
    }

  private:
    template <typename U>
    void construct_dispatch(U *, std::true_type)
    {
    }

    template <typename U>
    void construct_dispatch(U *p, std::false_type)
    {
        ::new (static_cast<void *>(p)) U();
    }
};

template <typename T>
using Vector = std::vector<T, Allocator<T>>;

template <typename Alloc>
inline ::bsoncxx::types::b_binary compress(const ::arrow::Buffer &buffer,
    std::vector<std::uint8_t, Alloc> *out, int compression_level)
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

template <typename Alloc>
inline ::bsoncxx::types::b_binary compress(
    const std::vector<std::uint8_t, Alloc> &buffer, Vector<std::uint8_t> *out,
    int compression_level)
{
    return compress(::arrow::Buffer(buffer.data(),
                        static_cast<std::int64_t>(buffer.size())),
        out, compression_level);
}

template <typename T, typename Alloc>
inline ::bsoncxx::types::b_binary compress(std::int64_t n, const T *data,
    std::vector<std::uint8_t, Alloc> *out, int compression_level)
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

template <typename T, typename Alloc>
inline void encode_datetime(
    std::int64_t n, const T *values, std::vector<std::uint8_t, Alloc> *out)
{
    if (n == 0) {
        out->clear();
        return;
    }

    out->resize(static_cast<std::size_t>(n) * sizeof(T));
    auto p = reinterpret_cast<T *>(out->data());
    p[0] = values[0];
    for (std::int64_t i = 1; i < n; ++i) {
        p[i] = values[i] - values[i - 1];
    }
}

template <typename T>
inline std::int64_t decode_datetime(
    const std::unique_ptr<::arrow::Buffer> &buffer)
{
    auto n = buffer->size() / static_cast<std::int64_t>(sizeof(T));
    auto p = reinterpret_cast<T *>(
        dynamic_cast<::arrow::MutableBuffer &>(*buffer).mutable_data());
    for (std::int64_t i = 1; i < n; ++i) {
        p[i] += p[i - 1];
    }

    return n;
}

/// \internal offsets and out will be of length n + 1, n is the array length
/// After encoding,
/// out[0] = 0
/// out[i] = offsets[i] - offsets[i - 1] for i >= 1, the counts of each segment
template <typename Alloc>
inline void encode_offsets(std::int64_t n, const std::int32_t *offsets,
    std::vector<std::uint8_t, Alloc> *out)
{
    out->resize((static_cast<std::size_t>(n) + 1) * sizeof(std::int32_t));
    auto p = reinterpret_cast<std::int32_t *>(out->data());
    p[0] = 0;
    for (std::int64_t i = 1; i <= n; ++i) {
        p[i] = offsets[i] - offsets[i - 1];
    }
}

/// \internal return the array length, the buffer is of n + 1 int32
inline std::int64_t decode_offsets(
    const std::unique_ptr<::arrow::Buffer> &counts)
{
    auto n = counts->size() / static_cast<std::int64_t>(sizeof(std::int32_t));
    auto p = reinterpret_cast<std::int32_t *>(
        dynamic_cast<::arrow::MutableBuffer &>(*counts).mutable_data());
    for (std::int64_t i = 1; i < n; ++i) {
        p[i] += p[i - 1];
    }

    return n - 1;
}

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_COMPRESS_HPP
