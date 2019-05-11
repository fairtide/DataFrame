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

#ifndef DATAFRAME_ARRAY_TYPE_STRUCT_HPP
#define DATAFRAME_ARRAY_TYPE_STRUCT_HPP

#include <dataframe/array/type/primitive.hpp>
#include <tuple>

#define DF_DEFINE_STRUCT_FIELD(Type, Index, Name, Getter, Setter)             \
    inline auto field_name(                                                   \
        Type *, std::integral_constant<std::size_t, Index>)                   \
    {                                                                         \
        return Name;                                                          \
    }                                                                         \
                                                                              \
    inline auto get_field(                                                    \
        const Type &__value, std::integral_constant<std::size_t, Index>)      \
    {                                                                         \
        return Getter(__value);                                               \
    }                                                                         \
                                                                              \
    template <typename __V>                                                   \
    inline void set_field(                                                    \
        Type &__value, __V &&__v, std::integral_constant<std::size_t, Index>) \
    {                                                                         \
        return Setter(__value, std::forward<__V>(__v));                       \
    }

namespace dataframe {

template <typename T, std::size_t N>
inline auto field_name(const T *, std::integral_constant<std::size_t, N>)
{
    return "Field" + std::to_string(N);
}

template <typename T, std::size_t N>
inline auto field_name()
{
    return field_name(
        static_cast<T *>(nullptr), std::integral_constant<std::size_t, N>());
}

template <typename T, std::size_t N>
inline auto get_field(const T &value, std::integral_constant<std::size_t, N>)
{
    return std::get<N>(value);
}

template <typename T, typename V, std::size_t N>
inline void set_field(T &value, V &&v, std::integral_constant<std::size_t, N>)
{
    std::get<N>(value) = v;
}

template <std::size_t N, typename T, typename Ref, typename Iter>
class FieldIterator
{
  public:
    using value_type = T;
    using reference = Ref;
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

    reference operator*() const { return get_field(*iter_, index_); }

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

  protected:
    static constexpr std::integral_constant<std::size_t, N> index_;

    Iter iter_;
};

template <std::size_t N, typename Iter>
inline auto field_iterator(Iter iter)
{
    using Ref =
        decltype(get_field(*iter, std::integral_constant<std::size_t, N>()));

    using T = std::remove_cv_t<std::remove_reference_t<Ref>>;

    return FieldIterator<N, T, Ref, Iter>(iter);
}

template <typename Iter, typename Member>
inline auto field_iterator(Iter iter, Member &&member,
    std::enable_if_t<std::is_member_object_pointer_v<Member>> * = nullptr)
{
    using Ref = decltype((*iter).*member);

    using T = std::remove_cv_t<std::remove_reference_t<Ref>>;

    using base = FieldIterator<0, T, Ref, Iter>;

    struct iterator : base {
        iterator(Iter iter, Member m)
            : base(iter)
            , mptr(m)
        {
        }

        Member mptr;

        Ref operator*() const { return (*this->iter_).*mptr; }
    };

    return iterator(iter, member);
}

template <typename Iter, typename Member>
inline auto field_iterator(Iter iter, Member &&member,
    std::enable_if_t<std::is_member_function_pointer_v<Member>> * = nullptr)
{
    using Ref = decltype(((*iter).*member)());

    using T = std::remove_cv_t<std::remove_reference_t<Ref>>;

    using base = FieldIterator<0, T, Ref, Iter>;

    struct iterator : base {
        iterator(Iter iter, Member m)
            : base(iter)
            , mptr(m)
        {
        }

        Member mptr;

        Ref operator*() const { return ((*this->iter_).*mptr)(); }
    };

    return iterator(iter, member);
}

struct StructBase {
};

template <typename... Types>
struct Struct final : StructBase {
    using data_type = std::tuple<Types...>;
};

template <std::size_t N, typename... Types>
using FieldType = std::tuple_element_t<N, std::tuple<Types...>>;

template <typename... Types>
struct TypeTraits<Struct<Types...>> {
    static std::shared_ptr<::arrow::StructType> data_type()
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;

        set_field<0>(fields, std::integral_constant<bool, 0 < nfields>());

        auto ret = ::arrow::struct_(fields);

        return std::static_pointer_cast<::arrow::StructType>(ret);
    }

    template <typename Name1, typename... Names>
    static std::shared_ptr<::arrow::StructType> data_type(
        Name1 &&name1, Names &&... names)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;

        set_field<0>(fields, std::integral_constant<bool, 0 < nfields>(),
            std::forward<Name1>(name1), std::forward<Names>(names)...);

        auto ret = ::arrow::struct_(fields);

        return std::static_pointer_cast<::arrow::StructType>(ret);
    }

    template <typename... Args>
    static std::unique_ptr<::arrow::StructBuilder> builder(Args &&... args)
    {
        return std::make_unique<::arrow::StructBuilder>(
            data_type(std::forward<Args>(args)...),
            ::arrow::default_memory_pool(), field_builders());
    }

    using ctype = std::tuple<CType<Types>...>;
    using array_type = ::arrow::StructArray;

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    template <std::size_t>
    static void set_field(
        std::vector<std::shared_ptr<::arrow::Field>> &, std::false_type)
    {
    }

    template <std::size_t N>
    static void set_field(
        std::vector<std::shared_ptr<::arrow::Field>> &fields, std::true_type)
    {
        using T = FieldType<N, Types...>;

        fields.push_back(::arrow::field(std::string(), make_data_type<T>()));

        set_field<N + 1>(
            fields, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t N, typename Name1, typename... Names>
    static void set_field(std::vector<std::shared_ptr<::arrow::Field>> &fields,
        std::true_type, Name1 &&name1, Names &&... names)
    {
        using T = FieldType<N, Types...>;

        fields.push_back(
            ::arrow::field(std::forward<Name1>(name1), make_data_type<T>()));

        set_field<N + 1>(fields,
            std::integral_constant<bool, N + 1 < nfields>(),
            std::forward<Names>(names)...);
    }

    static std::vector<std::shared_ptr<::arrow::ArrayBuilder>> field_builders()
    {
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> builders;
        set_builder<0>(builders, std::integral_constant<bool, 0 < nfields>());

        return builders;
    }

    template <std::size_t>
    static void set_builder(
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> &, std::false_type)
    {
    }

    template <std::size_t N>
    static void set_builder(
        std::vector<std::shared_ptr<::arrow::ArrayBuilder>> &builders,
        std::true_type)
    {
        builders.emplace_back(make_builder<FieldType<N, Types...>>());
        set_builder<N + 1>(
            builders, std::integral_constant<bool, N + 1 < nfields>());
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_STRUCT_HPP
