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

#ifndef DATAFRAME_TABLE_JOIN_HPP
#define DATAFRAME_TABLE_JOIN_HPP

#include <dataframe/table/bind.hpp>
#include <dataframe/table/select.hpp>
#include <unordered_map>

namespace dataframe {

enum class JoinType { Inner, Outer, Left, Right, Semi, Anti };

namespace internal {

class JoinVisitor : public ::arrow::ArrayVisitor
{
  public:
    std::shared_ptr<::arrow::Array> index;
    std::vector<std::int64_t> index1;
    std::vector<std::int64_t> index2;

    explicit JoinVisitor(JoinType kind, std::shared_ptr<::arrow::Array> array2)
        : kind_(kind)
        , array2_(std::move(array2))
    {
    }

    ::arrow::Status Visit(const ::arrow::Int8Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int16Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int32Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int64Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt8Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt16Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt32Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt64Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::FloatArray &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array1) override
    {
        const auto &array2 =
            *std::static_pointer_cast<::arrow::StringArray>(array2_);

        if (array1.null_count() != 0 || array2.null_count() != 0) {
            return ::arrow::Status::Invalid("Missing values in index columns");
        }

        auto view = [](auto &&array) {
            auto n = array.length();
            std::vector<std::remove_cv_t<
                std::remove_reference_t<decltype(array.GetView(0))>>>
                ret;
            ret.reserve(static_cast<std::size_t>(n));
            for (std::int64_t i = 0; i != n; ++i) {
                ret.emplace_back(array.GetView(i));
            }

            return ret;
        };

        auto view1 = view(array1);
        auto view2 = view(array2);

        return visit<::arrow::StringArray>(
            static_cast<std::int64_t>(view1.size()), view1.data(),
            static_cast<std::int64_t>(view2.size()), view2.data());
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array1)
    {
        const auto &array2 = *std::static_pointer_cast<ArrayType>(array2_);

        if (array1.null_count() != 0 || array2.null_count() != 0) {
            return ::arrow::Status::Invalid("Missing values in index columns");
        }

        return visit<ArrayType>(array1.length(), array1.raw_values(),
            array2.length(), array2.raw_values());
    }

    template <typename ArrayType, typename T>
    ::arrow::Status visit(
        std::int64_t n1, const T *v1, std::int64_t n2, const T *v2)
    {
        typename ::arrow::TypeTraits<typename ArrayType::TypeClass>::
            BuilderType builder(
                array2_->type(), ::arrow::default_memory_pool());

        switch (kind_) {
            case JoinType::Inner:
                ARROW_RETURN_NOT_OK(builder.Reserve(std::min(n1, n2)));
                break;
            case JoinType::Outer:
                ARROW_RETURN_NOT_OK(builder.Reserve(n1 + n2));
                break;
            case JoinType::Left:
                ARROW_RETURN_NOT_OK(builder.Reserve(n1));
                break;
            case JoinType::Right:
                ARROW_RETURN_NOT_OK(builder.Reserve(n2));
                break;
            case JoinType::Semi:
                ARROW_RETURN_NOT_OK(builder.Reserve(n1));
                break;
            case JoinType::Anti:
                ARROW_RETURN_NOT_OK(builder.Reserve(n1));
                break;
        }

        auto idx = [](auto &&n, auto &&v) {
            std::unordered_map<T, std::int64_t> ret;
            for (std::int64_t i = 0; i != n; ++i) {
                ret.emplace(v[i], i);
            }
            return ret;
        };

        switch (kind_) {
            case JoinType::Inner: {
                auto idx2 = idx(n2, v2);
                for (std::int64_t i = 0; i != n1; ++i) {
                    auto iter = idx2.find(v1[i]);
                    if (iter != idx2.end()) {
                        ARROW_RETURN_NOT_OK(builder.Append(v1[i]));
                        index1.emplace_back(i);
                        index2.emplace_back(iter->second);
                    }
                }
            } break;
            case JoinType::Outer: {
                auto idx2 = idx(n2, v2);
                std::vector<std::int64_t> only1;

                for (std::int64_t i = 0; i != n1; ++i) {
                    auto iter = idx2.find(v1[i]);
                    if (iter != idx2.end()) {
                        ARROW_RETURN_NOT_OK(builder.Append(v1[i]));
                        index1.emplace_back(i);
                        index2.emplace_back(iter->second);
                        idx2.erase(iter);
                    } else {
                        only1.emplace_back(i);
                    }
                }

                for (auto &&i : only1) {
                    ARROW_RETURN_NOT_OK(builder.Append(v1[i]));
                    index1.emplace_back(i);
                    index2.emplace_back(-1);
                }

                std::vector<std::int64_t> only2;
                for (auto &&vi : idx2) {
                    only2.emplace_back(vi.second);
                }
                std::sort(only2.begin(), only2.end());

                for (auto &&i : only2) {
                    ARROW_RETURN_NOT_OK(builder.Append(v2[i]));
                    index1.emplace_back(-1);
                    index2.emplace_back(i);
                }
            } break;
            case JoinType::Left: {
                auto idx2 = idx(n2, v2);
                for (std::int64_t i = 0; i != n1; ++i) {
                    auto iter = idx2.find(v1[i]);
                    ARROW_RETURN_NOT_OK(builder.Append(v1[i]));
                    index1.emplace_back(i);
                    if (iter != idx2.end()) {
                        index2.emplace_back(iter->second);
                    } else {
                        index2.emplace_back(-1);
                    }
                }
            } break;
            case JoinType::Right: {
                auto idx1 = idx(n1, v1);
                for (std::int64_t i = 0; i != n2; ++i) {
                    auto iter = idx1.find(v2[i]);
                    ARROW_RETURN_NOT_OK(builder.Append(v2[i]));
                    index2.emplace_back(i);
                    if (iter != idx1.end()) {
                        index1.emplace_back(iter->second);
                    } else {
                        index1.emplace_back(-1);
                    }
                }
            } break;
            case JoinType::Semi: {
                auto idx2 = idx(n2, v2);
                for (std::int64_t i = 0; i != n1; ++i) {
                    auto iter = idx2.find(v1[i]);
                    if (iter != idx2.end()) {
                        ARROW_RETURN_NOT_OK(builder.Append(v1[i]));
                        index1.emplace_back(i);
                    }
                }
            } break;
            case JoinType::Anti: {
                auto idx2 = idx(n2, v2);
                for (std::int64_t i = 0; i != n1; ++i) {
                    auto iter = idx2.find(v1[i]);
                    if (iter == idx2.end()) {
                        ARROW_RETURN_NOT_OK(builder.Append(v1[i]));
                        index1.emplace_back(i);
                    }
                }
            } break;
        }

        return builder.Finish(&index);
    }

  private:
    JoinType kind_;
    std::shared_ptr<::arrow::Array> array2_;
};

} // namespace internal

/// \brief Only include rows with keys that match in both `df1` and `df2`
inline DataFrame join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, JoinType kind = JoinType::Inner,
    bool make_unique = false)
{
    if (!df1[key]) {
        throw DataFrameException(
            "key " + key + " does not exist on left DataFrame");
    }

    if (!df2[key]) {
        throw DataFrameException(
            "key " + key + " does not exist on right DataFrame");
    }

    if (!df1[key].data()->type()->Equals(*df2[key].data()->type())) {
        throw DataFrameException("key " + key + " has different type");
    }

    bool left_only = kind == JoinType::Semi || kind == JoinType::Anti;

    if (!make_unique && !left_only) {
        auto ncol1 = df1.ncol();
        for (std::size_t i = 0; i != ncol1; ++i) {
            auto col1 = df1[i];
            auto col2 = df2[col1.name()];
            if (col2) {
                throw DataFrameException(
                    "key " + key + " exist in both DataFrames");
            }
        }
    }

    auto data1 = df1[key].data();
    auto data2 = df2[key].data();
    internal::JoinVisitor visitor(kind, data2);
    DF_ARROW_ERROR_HANDLER(data1->Accept(&visitor));

    DataFrame index;
    index[key] = visitor.index;

    auto ret1 = select(df1, visitor.index1.begin(), visitor.index1.end());
    auto ret2 = select(df2, visitor.index2.begin(), visitor.index2.end());
    ret1[key].remove();
    ret2[key].remove();

    if (make_unique && !left_only) {
        auto ncol1 = ret1.ncol();
        for (std::size_t i = 0; i != ncol1; ++i) {
            auto col1 = ret1[i];
            auto col2 = ret2[col1.name()];
            if (col2) {
                col1.rename(col1.name() + "_1");
                col2.rename(col2.name() + "_2");
            }
        }
    }

    return bind_cols({index, ret1, ret2});
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_JOIN_HPP
