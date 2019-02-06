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

#include <dataframe/dataframe.hpp>

namespace dataframe {

namespace internal {

class BindRowsVisitor : public ::arrow::ArrayVisitor
{
  public:
    explicit BindRowsVisitor(std::unique_ptr<::arrow::ArrayBuilder> *builder)
        : builder_(builder)
    {
    }

    ::arrow::Status Visit(const ::arrow::Int8Array &values) final
    {
        return Append<::arrow::Int8Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::Int16Array &values) final
    {
        return Append<::arrow::Int16Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::Int32Array &values) final
    {
        return Append<::arrow::Int32Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::Int64Array &values) final
    {
        return Append<::arrow::Int64Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::UInt8Array &values) final
    {
        return Append<::arrow::UInt8Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::UInt16Array &values) final
    {
        return Append<::arrow::UInt16Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::UInt32Array &values) final
    {
        return Append<::arrow::UInt32Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::UInt64Array &values) final
    {
        return Append<::arrow::UInt64Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::FloatArray &values) final
    {
        return Append<::arrow::FloatBuilder>(values);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &values) final
    {
        return Append<::arrow::DoubleBuilder>(values);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &values) final
    {
        return Append<::arrow::Date32Builder>(values);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &values) final
    {
        auto builder = GetBuilder<::arrow::TimestampBuilder>(values.type());

        if (!builder->type()->Equals(values.type())) {
            throw DataFrameException("Mismatch of timestamp type");
        }

        return AppendValues(builder, values);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &values) final
    {
        auto builder = GetBuilder<::arrow::StringBuilder>(values.type());

        auto n = values.length();
        for (std::int64_t i = 0; i != n; ++i) {
            if (values.IsValid(i)) {
                FT_ARROW_ERROR_HANDLER(builder->Append(values.GetView(i)));
            } else {
                FT_ARROW_ERROR_HANDLER(builder->AppendNull());
            }
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &values) final
    {
        auto n = values.length();
        auto dict = values.dictionary();
        if (dict->type()->id() != ::arrow::Type::STRING) {
            throw DataFrameException(
                "Dictionary other than string type not supported");
        }

        auto builder = GetBuilder<::arrow::StringDictionaryBuilder>();
        auto dptr = std::static_pointer_cast<::arrow::StringArray>(dict);
        auto iptr = values.indices();
        auto sidx = GetIndex(values);

        for (std::int64_t i = 0; i != n; ++i) {
            if (iptr->IsValid(i)) {
                FT_ARROW_ERROR_HANDLER(builder->Append(
                    dptr->GetView(sidx[static_cast<std::size_t>(i)])));
            } else {
                FT_ARROW_ERROR_HANDLER(builder->AppendNull());
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    template <typename Builder, typename ArrayType, typename... Args>
    ::arrow::Status Append(const ArrayType &values, Args &&... args) const
    {
        return AppendValues(
            GetBuilder<Builder>(std::forward<Args>(args)...), values);
    }

    template <typename Builder, typename ArrayType>
    ::arrow::Status AppendValues(
        Builder *builder, const ArrayType &values) const
    {
        auto n = values.length();
        auto v = values.raw_values();
        if (values.null_count() == 0) {
            FT_ARROW_ERROR_HANDLER(builder->AppendValues(v, n));
        } else {
            std::vector<bool> bits(static_cast<std::size_t>(n));
            for (std::int64_t i = 0; i != n; ++i) {
                bits[static_cast<std::size_t>(i)] = values.IsValid(i);
            }
            FT_ARROW_ERROR_HANDLER(builder->AppendValues(v, n, bits));
        }

        return ::arrow::Status::OK();
    }

    template <typename Builder, typename... Args>
    Builder *GetBuilder(Args &&... args) const
    {
        if (*builder_ == nullptr) {
            *builder_ = std::make_unique<Builder>(
                std::forward<Args>(args)..., ::arrow::default_memory_pool());
        }

        auto builder = dynamic_cast<Builder *>(builder_->get());

        if (builder == nullptr) {
            throw DataFrameException("Incorrect builder type");
        }

        return builder;
    }

  private:
    std::unique_ptr<::arrow::ArrayBuilder> *builder_;
};

inline std::shared_ptr<::arrow::Table> bind_rows(
    const std::vector<std::shared_ptr<::arrow::Table>> &tables)
{
    for (auto &tbl : tables) {
        if (tbl == nullptr) {
            throw DataFrameException("Null pointer to tables");
        }
    }

    if (tables.empty()) {
        return nullptr;
    }

    struct ColumnBuilder {
        ColumnBuilder(std::string nm, ::arrow::Type::type tid)
            : name(std::move(nm))
            , type_id(tid)
        {
        }

        std::string name;
        ::arrow::Type::type type_id;
        std::unique_ptr<::arrow::ArrayBuilder> builder;
        std::int64_t null_count = 0;
    };

    std::vector<ColumnBuilder> builders;
    for (auto i = 0; i != tables.front()->num_columns(); ++i) {
        auto col = tables.front()->column(i);
        builders.emplace_back(col->name(), col->type()->id());
    }

    for (std::size_t i = 1; i < tables.size(); ++i) {
        auto &tbl = tables[i];

        if (tbl->num_columns() != tables.front()->num_columns()) {
            throw DataFrameException("Mismatched width of tables");
        }

        for (auto j = 0; j != tbl->num_columns(); ++j) {
            auto col = tbl->column(j);
            auto name = col->name();
            auto type_id = col->type()->id();
            auto iter =
                std::find_if(builders.begin(), builders.end(), [&](auto &&v) {
                    return v.name == name && v.type_id == type_id;
                });

            if (iter == builders.end()) {
                throw DataFrameException(
                    col->name() + " not found in first table");
            }
        }
    }

    for (auto &tbl : tables) {
        for (auto i = 0; i != tbl->num_columns(); ++i) {
            auto col = tbl->column(i);
            auto name = col->name();
            auto type_id = col->type()->id();
            auto iter =
                std::find_if(builders.begin(), builders.end(), [&](auto &&v) {
                    return v.name == name && v.type_id == type_id;
                });
            for (auto &&data : col->data()->chunks()) {
                internal::ArrowBindRowsVisitor visitor(&iter->builder);
                FT_ARROW_ERROR_HANDLER(data->Accept(&visitor));
            }
        }
    }

    std::vector<std::shared_ptr<::arrow::Column>> columns;
    columns.reserve(builders.size());
    std::shared_ptr<::arrow::Array> values;
    for (auto &&builder : builders) {
        FT_ARROW_ERROR_HANDLER(builder.builder->Finish(&values));
        values = Normalize(values);
        columns.push_back(std::make_shared<::arrow::Column>(
            ::arrow::field(
                builder.name, values->type(), values->null_count() != 0),
            values));
    }

    std::vector<std::shared_ptr<::arrow::Field>> fields;
    fields.reserve(builders.size());
    for (auto &&c : columns) {
        fields.push_back(c->field());
    }

    return ::arrow::Table::Make(
        std::make_shared<::arrow::Schema>(fields), columns);
}

inline std::shared_ptr<::arrow::Table> bind_columns(
    const std::vector<std::shared_ptr<::arrow::Table>> &tables)
{
    for (auto &tbl : tables) {
        if (tbl == nullptr) {
            throw DataFrameException("Null pointer to tables");
        }
    }

    if (tables.empty()) {
        return nullptr;
    }

    auto n = tables.front()->num_rows();
    std::set<std::string> keys;
    std::vector<std::shared_ptr<::arrow::Column>> columns;
    std::vector<std::shared_ptr<::arrow::Field>> fields;
    for (auto &tbl : tables) {
        if (tbl->num_rows() != n) {
            throw DataFrameException("Tables have different number of rows");
        }
        auto ncols = tbl->num_columns();
        for (auto i = 0; i != ncols; ++i) {
            auto col = tbl->column(i);
            if (!keys.insert(col->name()).second) {
                throw DataFrameException("Duplicate key " + col->name());
            }
            columns.push_back(col);
            fields.push_back(col->field());
        }
    }

    return ::arrow::Table::Make(
        std::make_shared<::arrow::Schema>(fields), columns);
}

} // namespace internal

template <typename InputIter>
inline DataFrame bind_rows(InputIter first, InputIter last)
{
    std::vector<std::shared_ptr<::arrow::Table>> tables;
    while (first != last) {
        tables.emplace_back(first++->table());
    }
    return DataFrame(internal::bind_rows(tables));
}

template <typename InputIter>
inline DataFrame bind_columns(InputIter first, InputIter last)
{
    std::vector<std::shared_ptr<::arrow::Table>> tables;
    while (first != last) {
        tables.emplace_back(first++->table());
    }
    return DataFrame(internal::bind_columns(tables));
}

} // namespace dataframe

#endif // DATAFRAME_CONCATENATE_HPP
