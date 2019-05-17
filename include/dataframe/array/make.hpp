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

#ifndef DATAFRAME_ARRAY_MAKE_HPP
#define DATAFRAME_ARRAY_MAKE_HPP

#include <dataframe/array/make/bool.hpp>
#include <dataframe/array/make/datetime.hpp>
#include <dataframe/array/make/dict.hpp>
#include <dataframe/array/make/list.hpp>
#include <dataframe/array/make/null.hpp>
#include <dataframe/array/make/primitive.hpp>
#include <dataframe/array/make/string.hpp>
#include <dataframe/array/make/struct.hpp>

namespace dataframe {

namespace internal {

template <typename Iter>
inline std::shared_ptr<::arrow::Array> set_mask(
    const std::shared_ptr<::arrow::Array> &array, Iter iter,
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    auto data = array->data()->Copy();
    auto length = data->length;
    auto &null_bitmap = data->buffers.at(0);

    DF_ARROW_ERROR_HANDLER(::arrow::AllocateBuffer(
        pool, ::arrow::BitUtil::BytesForBits(length), &null_bitmap));

    auto bits =
        dynamic_cast<::arrow::MutableBuffer &>(*null_bitmap).mutable_data();

    std::int64_t null_count = 0;
    for (std::int64_t i = 0; i != length; ++i, ++iter) {
        auto is_valid = *iter;
        null_count += !is_valid;
        ::arrow::BitUtil::SetBitTo(bits, i, is_valid);
    }

    data->null_count = null_count;

    return ::arrow::MakeArray(data);
}

template <typename V, typename R, typename Iter, typename Member>
class MemberObjectIterator
{
  public:
    using value_type = V;
    using reference = R;
    using iterator_category =
        typename std::iterator_traits<Iter>::iterator_category;

    MemberObjectIterator(Iter iter, Member ptr)
        : iter_(iter)
        , ptr_(ptr)
    {
    }

    reference operator*() const { return (*iter_).*ptr_; }

    DF_DEFINE_ITERATOR_MEMBERS(MemberObjectIterator, iter_)

  private:
    Iter iter_;
    Member ptr_;
};

template <typename V, typename R, typename Iter, typename Member>
class MemberFunctionIterator
{
  public:
    using value_type = V;
    using reference = R;
    using iterator_category =
        typename std::iterator_traits<Iter>::iterator_category;

    MemberFunctionIterator(Iter iter, Member ptr)
        : iter_(iter)
        , ptr_(ptr)
    {
    }

    reference operator*() const { return ((*iter_).*ptr_)(); }

    DF_DEFINE_ITERATOR_MEMBERS(MemberFunctionIterator, iter_)

  private:
    Iter iter_;
    Member ptr_;
};

template <typename V, typename R, typename Iter, typename Getter>
class MemberGetterIterator
{
  public:
    using value_type = V;
    using reference = R;
    using iterator_category =
        typename std::iterator_traits<Iter>::iterator_category;

    MemberGetterIterator(Iter iter, Getter get)
        : iter_(iter)
        , get_(get)
    {
    }

    reference operator*() const { return get_(*iter_); }

    DF_DEFINE_ITERATOR_MEMBERS(MemberGetterIterator, iter_)

  private:
    Iter iter_;
    Getter get_;
};

} // namespace internal

template <typename Iter, typename... Args>
inline EnableMakeArray<
    !std::is_same_v<typename std::iterator_traits<Iter>::value_type, void>>
make_array(Iter first, Iter last, Args &&... args)
{
    using T = std::remove_cv_t<std::remove_reference_t<decltype(*first)>>;

    return make_array<T>(first, last, std::forward<Args>(args)...);
}

template <typename T, typename Iter, typename Member>
inline EnableMakeArray<std::is_member_object_pointer_v<Member>> make_array(
    Iter first, Iter last, Member member,
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    using R = decltype((*first).*member);
    using V = std::remove_cv_t<std::remove_reference_t<R>>;
    using I = internal::MemberObjectIterator<V, R, Iter, Member>;

    return make_array<T>(I(first, member), I(last, member), pool);
}

template <typename T, typename Iter, typename Member>
inline EnableMakeArray<std::is_member_function_pointer_v<Member>> make_array(
    Iter first, Iter last, Member member,
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    using R = decltype(((*first).*member)());
    using V = std::remove_cv_t<std::remove_reference_t<R>>;
    using I = internal::MemberFunctionIterator<V, R, Iter, Member>;

    return make_array<T>(I(first, member), I(last, member), pool);
}

template <typename T, typename Iter, typename Getter>
inline EnableMakeArray<!std::is_member_pointer_v<Getter> &&
    std::is_invocable_v<Getter, decltype(*std::declval<Iter>())>>
make_array(Iter first, Iter last, Getter getter,
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    using R = decltype(getter(*first));
    using V = std::remove_cv_t<std::remove_reference_t<R>>;
    using I = internal::MemberGetterIterator<V, R, Iter, Getter>;

    return make_array<T>(I(first, getter), I(last, getter), pool);
}

template <typename T, typename Iter, typename Valid, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    Iter first, Iter last, Valid valid, Args &&... args)
{
    return internal::set_mask(
        make_array<T>(first, last, std::forward<Args>(args)...), valid);
}

template <typename T, typename Iter, typename Alloc, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    const std::vector<bool, Alloc> &valid, Args &&... args)
{
    return internal::set_mask(
        make_array<T>(first, last, std::forward<Args>(args)...),
        valid.begin());
}

template <typename T, typename V, typename Alloc>
inline std::shared_ptr<::arrow::Array> make_array(
    const std::vector<V, Alloc> &vec)
{
    return make_array<T>(vec.begin(), vec.end());
}

template <typename T, typename V, typename Alloc, typename AllocM>
inline std::shared_ptr<::arrow::Array> make_array(
    const std::vector<V, Alloc> &vec, const std::vector<bool, AllocM> &mask)
{
    return make_array<T>(vec.begin(), vec.end(), mask.begin());
}

template <typename T, typename V, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    std::size_t n, const V *data, Args &&... args)
{
    return make_array<T>(data, data + n, std::forward<Args>(args)...);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_HPP
