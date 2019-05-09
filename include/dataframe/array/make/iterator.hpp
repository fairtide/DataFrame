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

#ifndef DATAFRAME_ARRAY_MAKE_ITERATOR_HPP
#define DATAFRAME_ARRAY_MAKE_ITERATOR_HPP

#include <iterator>
#include <type_traits>

#define DF_DEFINE_STRUCT_FIELD(T, I, name, getter)                                   \
    template <typename Iter>                                                  \
    auto field_name(Iter, std::integral_constant<std::size_t, I>,             \
        std::enable_if_t<std::is_same_v<T,                                    \
            typename std::iterator_traits<Iter>::value_type>> * = nullptr)    \
    {                                                                         \
        return name;                                                          \
    }                                                                         \
                                                                              \
    template <typename Iter>                                                  \
    auto field_iterator(Iter iter, std::integral_constant<std::size_t, I>,    \
        std::enable_if_t<std::is_same_v<T,                                    \
            typename std::iterator_traits<Iter>::value_type>> * = nullptr)    \
    {                                                                         \
        using ::dataframe::field_iterator;                                    \
        return field_iterator(iter, getter);                                  \
    }

template <typename Iter, std::size_t N>
auto field_name(Iter, std::integral_constant<std::size_t, N>);

namespace dataframe {

template <typename Derived, typename Iter, typename Ref>
class FieldIterator
{
  public:
    using reference = Ref;
    using value_type = std::remove_reference_t<reference>;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type *;
    using iterator_category =
        typename std::iterator_traits<Iter>::iterator_category;

    FieldIterator() noexcept = default;

    FieldIterator(Iter iter) noexcept
        : iter_(std::move(iter))
    {
    }

    FieldIterator(const FieldIterator &) noexcept = default;
    FieldIterator(FieldIterator &&) noexcept = default;
    FieldIterator &operator=(const FieldIterator &) noexcept = default;
    FieldIterator &operator=(FieldIterator &&) noexcept = default;

    reference operator*() const noexcept(
        noexcept(static_cast<const Derived *>(this)->dereference(iter_)))
    {
        return static_cast<const Derived *>(this)->dereference(iter_);
    }

    FieldIterator &operator++() noexcept
    {
        ++iter_;

        return *this;
    }

    FieldIterator operator++(int) noexcept
    {
        auto ret = *this;
        ++(*this);

        return ret;
    }

    FieldIterator &operator--() noexcept
    {
        --iter_;

        return *this;
    }

    FieldIterator operator--(int) noexcept
    {
        auto ret = *this;
        --(*this);

        return ret;
    }

    FieldIterator &operator+=(difference_type n) noexcept
    {
        iter_ += n;

        return *this;
    }

    FieldIterator &operator-=(difference_type n) noexcept
    {
        return *this += -n;
    }

    FieldIterator operator+(difference_type n) const noexcept
    {
        auto ret = *this;
        ret += n;

        return ret;
    }

    FieldIterator operator-(difference_type n) const noexcept
    {
        auto ret = *this;
        ret -= n;

        return ret;
    }

    friend FieldIterator operator+(
        difference_type n, const FieldIterator &iter) noexcept
    {
        return iter + n;
    }

    friend difference_type operator-(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return x.iter_ - y.iter_;
    }

    reference operator[](difference_type n) const noexcept
    {
        return *(*this + n);
    }

    friend bool operator==(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return x.iter_ == y.iter_;
    }

    friend bool operator!=(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return !(x == y);
    }

    friend bool operator<(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return x.iter_ < y.iter_;
    }

    friend bool operator>(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return y < x;
    }

    friend bool operator<=(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return !(y < x);
    }

    friend bool operator>=(
        const FieldIterator &x, const FieldIterator &y) noexcept
    {
        return !(x < y);
    }

  private:
    Iter iter_;
};

template <typename Iter, typename Member>
auto field_iterator(Iter iter, Member &&member,
    std::enable_if_t<std::is_member_object_pointer_v<Member>> * = nullptr)
{
    using Ref = decltype((*iter).*member);

    struct iterator : FieldIterator<iterator, Iter, Ref> {
        iterator(Iter iter, Member mem)
            : FieldIterator<iterator, Iter, Ref>(iter)
            , mptr(mem)
        {
        }

        Member mptr;

        Ref dereference(Iter iter) const { return (*iter).*mptr; }
    };

    return iterator(iter, std::forward<Member>(member));
}

template <typename Iter, typename Member>
auto field_iterator(Iter iter, Member &&member,
    std::enable_if_t<std::is_member_function_pointer_v<Member>> * = nullptr)
{
    using Ref = decltype(((*iter).*member)());

    struct iterator : FieldIterator<iterator, Iter, Ref> {
        iterator(Iter iter, Member mem)
            : FieldIterator<iterator, Iter, Ref>(iter)
            , mptr(mem)
        {
        }

        Member mptr;

        Ref dereference(Iter iter) const { return ((*iter).*mptr)(); }
    };

    return iterator(iter, std::forward<Member>(member));
}

template <typename Iter, typename Getter>
auto field_iterator(Iter iter, Getter &&getter,
    std::enable_if_t<!std::is_member_pointer_v<Getter>> * = nullptr)
{
    using Ref = decltype(getter(*iter));

    struct iterator : FieldIterator<iterator, Iter, Ref> {
        iterator(Iter iter, Getter v)
            : FieldIterator<iterator, Iter, Ref>(iter)
            , get(std::move(v))
        {
        }

        Getter get;

        Ref dereference(Iter iter) const { return get(*iter); }
    };

    return iterator(iter, std::forward<Getter>(getter));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_ITERATOR_HPP
