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

#include <dataframe/array/make/datetime.hpp>
#include <dataframe/array/make/list.hpp>
#include <dataframe/array/make/primitive.hpp>
#include <dataframe/array/make/string.hpp>
#include <dataframe/array/make/struct.hpp>

namespace dataframe {

namespace internal {

template <typename Iter>
inline std::shared_ptr<::arrow::Array> set_mask(
    const std::shared_ptr<::arrow::Array> &array, Iter iter)
{
    auto data = array->data()->Copy();
    auto length = data->length;
    auto &null_bitmap = data->buffers.at(0);

    DF_ARROW_ERROR_HANDLER(
        ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
            ::arrow::BitUtil::BytesForBits(length), &null_bitmap));

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

template <typename Iter>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last)
{
    using T = std::remove_cv_t<std::remove_reference_t<decltype(*first)>>;

    return make_array<T>(first, last);
}

template <typename T, typename Iter, typename Member>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    Member &&member,
    std::enable_if_t<std::is_member_object_pointer_v<Member>> * = nullptr)
{
    using R = decltype((*first).*member);
    using V = std::remove_cv_t<std::remove_reference_t<R>>;
    using I = internal::MemberObjectIterator<V, R, Iter, Member>;

    return make_array<T>(I(first, member), I(last, member));
}

template <typename Iter, typename Member>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    Member &&member,
    std::enable_if_t<std::is_member_object_pointer_v<Member>> * = nullptr)
{
    using T =
        std::remove_cv_t<std::remove_reference_t<decltype((*first).member)>>;

    return make_array<T>(first, last, std::forward<Member>(member));
}

template <typename T, typename Iter, typename Member>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    Member &&member,
    std::enable_if_t<std::is_member_function_pointer_v<Member>> * = nullptr)
{
    using R = decltype(((*first).*member)());
    using V = std::remove_cv_t<std::remove_reference_t<R>>;
    using I = internal::MemberFunctionIterator<V, R, Iter, Member>;

    return make_array<T>(I(first, member), I(last, member));
}

template <typename Iter, typename Member>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    Member &&member,
    std::enable_if_t<std::is_member_function_pointer_v<Member>> * = nullptr)
{
    using T = std::remove_cv_t<
        std::remove_reference_t<decltype(((*first).member)())>>;

    return make_array<T>(first, last, std::forward<Member>(member));
}

template <typename T, typename Iter, typename Getter>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    Getter &&getter,
    std::enable_if_t<!std::is_member_pointer_v<Getter> &&
        std::is_invocable_v<Getter, decltype(*first)>> * = nullptr)
{
    using R = decltype(getter(*first));
    using V = std::remove_cv_t<std::remove_reference_t<R>>;
    using I = internal::MemberGetterIterator<V, R, Iter, Getter>;

    return make_array<T>(I(first, getter), I(last, getter));
}

template <typename Iter, typename Getter>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    Getter &&getter,
    std::enable_if_t<!std::is_member_pointer_v<Getter> &&
        std::is_invocable_v<Getter, decltype(*first)>> * = nullptr)
{
    using T =
        std::remove_cv_t<std::remove_reference_t<decltype(getter(*first))>>;

    return make_array<T>(first, last, std::forward<Getter>(getter));
}

template <typename T, typename Iter, typename Valid, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    Iter first, Iter last, Valid valid, Args &&... args)
{
    return internal::set_mask(
        make_array<T>(first, last, std::forward<Args>(args)...), valid);
}

template <typename Iter, typename Valid, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    Iter first, Iter last, Valid valid, Args &&... args)
{
    return internal::set_mask(
        make_array(first, last, std::forward<Args>(args)...), valid);
}

template <typename T, typename Iter, typename Alloc, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    const std::vector<bool, Alloc> &valid, Args &&... args)
{
    return internal::set_mask(
        make_array<T>(first, last, std::forward<Args>(args)...),
        valid.begin());
}

template <typename Iter, typename Alloc, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last,
    const std::vector<bool, Alloc> &valid, Args &&... args)
{
    return internal::set_mask(
        make_array(first, last, std::forward<Args>(args)...), valid.begin());
}

template <typename T, typename V, typename Alloc, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    const std::vector<V, Alloc> &vec, Args &&... args)
{
    return make_array<T>(vec.begin(), vec.end(), std::forward<Args>(args)...);
}

template <typename T, typename V, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    std::size_t n, const V *data, Args &&... args)
{
    return make_array<T>(data, data + n, std::forward<Args>(args)...);
}

template <typename V, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    std::size_t n, const V *data, Args &&... args)
{
    return make_array(data, data + n, std::forward<Args>(args)...);
}

template <typename T>
class Repeat
{
  public:
    using value_type = T;

    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    using reference = const value_type &;
    using const_reference = reference;

    class iterator
    {
      public:
        using value_type = T;
        using reference = const value_type &;
        using iterator_category = std::random_access_iterator_tag;

        reference operator*() const noexcept { return ptr_; }

        DF_DEFINE_ITERATOR_MEMBERS(iterator, pos_)

      private:
        friend Repeat;

        iterator(pointer ptr, size_type pos)
            : ptr_(ptr)
            , pos_(static_cast<std::ptrdiff_t>(pos))
        {
        }

        pointer ptr_;
        difference_type pos_;
    };

    using pointer = iterator;
    using const_pointer = pointer;

    using const_iterator = iterator;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    Repeat(const value_type &value, size_type size)
        : value_(value)
        , size_(size)
    {
    }

    Repeat(const Repeat &) = delete;
    Repeat(Repeat &&) noexcept = delete;

    Repeat &operator=(const Repeat &) = delete;
    Repeat &operator=(Repeat &&) noexcept = delete;

    const_reference operator[](size_type) const noexcept { return value_; }

    const_reference at(size_type pos) const
    {
        if (pos >= size_) {
            throw std::out_of_range("dataframe::Repeat::at");
        }

        return operator[](pos);
    }

    const_reference front() const noexcept { return value_; }

    const_reference back() const noexcept { return value_; }

    // Iterators

    const_iterator begin() const noexcept { return iterator(&value_, 0); }
    const_iterator end() const noexcept { return iterator(&value_, size_); }

    const_iterator cbegin() const noexcept { return begin(); }
    const_iterator cend() const noexcept { return end(); }

    const_reverse_iterator rbegin() const noexcept
    {
        return const_reverse_iterator{end()};
    }

    const_reverse_iterator rend() const noexcept
    {
        return const_reverse_iterator{begin()};
    }

    const_reverse_iterator crbegin() const noexcept { return rbegin(); }
    const_reverse_iterator crend() const noexcept { return rend(); }

    // Capacity

    bool empty() const noexcept { return size_ == 0; }

    size_type size() const noexcept { return size_; }

    size_type max_size() const noexcept
    {
        return std::numeric_limits<size_type>::max() / sizeof(value_type);
    }

    template <typename OutputIter, typename Setter>
    OutputIter set(OutputIter out, Setter &&setter)
    {
        for (std::size_t i = 0; i != size_; ++i, ++out) {
            setter(value_, out);
        }

        return out;
    }

  private:
    value_type value_;
    size_type size_;
};

template <typename T>
inline Repeat<T> repeat(const T &value, std::size_t size)
{
    return Repeat<T>(value, size);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_HPP
