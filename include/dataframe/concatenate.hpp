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

#ifndef DATAFRAME_CONCATENATE_HPP
#define DATAFRAME_CONCATENATE_HPP

#include <dataframe/data_frame.hpp>

namespace dataframe {

namespace internal {

class BindRowsVisitor final : public ::arrow::ArrayVisitor
{
  public:
    explicit BindRowsVisitor(std::unique_ptr<::arrow::ArrayBuilder> *builder)
        : builder_(builder)
    {
    }

    ::arrow::Status Visit(const ::arrow::Int8Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int16Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt8Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt16Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::FloatArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        if (*builder_ == nullptr) {
            *builder_ = std::make_unique<::arrow::StringBuilder>(
                array.type(), ::arrow::default_memory_pool());
        }

        auto builder = dynamic_cast<::arrow::StringBuilder *>(builder_->get());
        if (builder == nullptr || !builder->type()->Equals(array.type())) {
            return ::arrow::Status::Invalid("Type mismatch");
        }

        auto n = array.length();
        for (std::int64_t i = 0; i != n; ++i) {
            if (array.IsValid(i)) {
                ARROW_RETURN_NOT_OK(builder->Append(array.GetView(i)));
            } else {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array) const
    {
        using BuilderType = typename ::arrow::TypeTraits<
            typename ArrayType::TypeClass>::BuilderType;

        if (*builder_ == nullptr) {
            *builder_ = std::make_unique<BuilderType>(
                array.type(), ::arrow::default_memory_pool());
        }

        auto builder = dynamic_cast<BuilderType *>(builder_->get());
        if (builder == nullptr || !builder->type()->Equals(array.type())) {
            return ::arrow::Status::Invalid("Type mismatch");
        }

        if (array.null_count() == 0) {
            return builder->AppendValues(array.raw_values(), array.length());
        } else {
            ArrayMask mask(static_cast<std::size_t>(array.length()),
                array.null_bitmap_data());
            return builder->AppendValues(
                array.raw_values(), array.length(), mask.data());
        }
    }

  private:
    std::unique_ptr<::arrow::ArrayBuilder> *builder_;
};

} // namespace internal

template <typename InputIter>
inline DataFrame bind_rows(InputIter first, InputIter last)
{
    struct ColumnBuilder {
        ColumnBuilder(std::string nm)
            : name(std::move(nm))
        {
        }

        std::string name;
        std::unique_ptr<::arrow::ArrayBuilder> builder;
        CategoricalArray categorical;
    };

    DataFrame ret;

    while (first != last && first->empty()) {
        ++first;
    }

    if (first == last) {
        return ret;
    }

    std::vector<ColumnBuilder> builders;

    const auto &df0 = *first++;
    auto ncol = df0.ncol();
    for (std::size_t i = 0; i != ncol; ++i) {
        auto col = df0[i];
        builders.emplace_back(col.name());
        auto &b = builders.back();
        if (col.dtype() == DataType::Categorical) {
            b.categorical = col.template as_view<CategoricalArray>();
        } else {
            internal::BindRowsVisitor visitor(&b.builder);
            DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));
        }
    }

    while (first != last) {
        const auto &df = *first++;

        if (df.empty()) {
            continue;
        }

        ncol = df.ncol();
        if (ncol != builders.size()) {
            throw DataFrameException("Number of columns mismatch");
        }

        for (auto &&b : builders) {
            auto col = df[b.name];
            if (!col) {
                throw DataFrameException("Column " + b.name + " not found");
            }

            if (b.builder == nullptr && col.dtype() == DataType::Categorical) {
                auto v = col.template as_view<CategoricalArray>();
                const auto &m = v.mask();
                for (std::size_t i = 0; i != v.size(); ++i) {
                    if (m[i]) {
                        b.categorical.emplace_back(v[i]);
                    } else {
                        b.categorical.emplace_back();
                    }
                }
            } else {
                internal::BindRowsVisitor visitor(&b.builder);
                DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));
            }
        }
    }

    for (auto &&b : builders) {
        if (b.builder == nullptr) {
            ret[b.name] = b.categorical;
        } else {
            ret[b.name] = *b.builder;
        }
    }

    return ret;
}

template <typename InputIter>
inline DataFrame bind_cols(InputIter first, InputIter last)
{
    DataFrame ret;

    while (first != last) {
        const auto &df = *first++;

        if (df.empty()) {
            continue;
        }

        auto ncol = df.ncol();
        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];
            if (ret[col.name()]) {
                throw DataFrameException(
                    "Duplicate column name " + col.name());
            }
            ret[col.name()] = col;
        }
    }

    return ret;
}

inline DataFrame bind_rows(const std::vector<DataFrame> &dfs)
{
    return bind_rows(dfs.begin(), dfs.end());
}

inline DataFrame bind_cols(const std::vector<DataFrame> &dfs)
{
    return bind_cols(dfs.begin(), dfs.end());
}

} // namespace dataframe

#endif // DATAFRAME_CONCATENATE_HPP
