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

#define DF_DEFINE_STRUCT_FIELD(T, N, name, getter)                            \
    template <typename Iter>                                                  \
    auto field_name(Iter, std::integral_constant<std::size_t, N>,             \
        std::enable_if_t<std::is_constructible_v<T, decltype(*Iter())>> * =   \
            nullptr)                                                          \
    {                                                                         \
        return name;                                                          \
    }                                                                         \
                                                                              \
    template <typename Iter>                                                  \
    auto field_iterator(Iter iter, std::integral_constant<std::size_t, N>,    \
        std::enable_if_t<std::is_constructible_v<T, decltype(*Iter())>> * =   \
            nullptr)                                                          \
    {                                                                         \
        using ::dataframe::field_iterator;                                    \
        return field_iterator(iter, getter);                                  \
    }

namespace dataframe {

namespace internal {

template <typename T>
struct UnwrapReference {
    using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

template <typename T>
struct UnwrapReference<std::reference_wrapper<T>> {
    using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

} // namespace internal

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
    std::enable_if_t<!std::is_member_pointer_v<Getter> &&
        std::is_invocable_v<Getter, decltype(*Iter())>> * = nullptr)
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

template <typename Iter, std::size_t N>
auto field_name(Iter, std::integral_constant<std::size_t, N>)
{
    return "Field" + std::to_string(N);
}

template <typename Iter, std::size_t N>
auto field_iterator(Iter iter, std::integral_constant<std::size_t, N>)
{
    using T = typename internal::UnwrapReference<
        typename std::iterator_traits<Iter>::value_type>::type;

    using std::get;

    return field_iterator(iter, [](const T &v) { return get<N>(v); });
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
