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

#ifndef DATAFRAME_ARRAY_CATEGORICAL_HPP
#define DATAFRAME_ARRAY_CATEGORICAL_HPP

#include <dataframe/internal/categorical_visitor.hpp>
#include <dataframe/array/mask.hpp>
#include <dataframe/error.hpp>
#include <arrow/api.h>
#include <unordered_map>

namespace dataframe {

class CategoricalArray : public ArrayMask
{
  public:
    CategoricalArray() = default;

    void push_back(const std::string_view &str)
    {
        auto iter = pool_.find(str);

        if (iter != pool_.end()) {
            index_.push_back(iter->second);
        } else {
            index_.push_back(static_cast<std::int32_t>(levels_.size()));
            levels_.emplace_back(str);
            // TODO should be safe to more efficiently just emplace one element
            for (std::size_t i = 0; i != levels_.size(); ++i) {
                pool_[levels_[i]] = static_cast<std::int32_t>(i);
            }
        }

        if (!mask_.empty()) {
            mask_.push_back(true);
        }
    }

    template <typename T, typename... Args>
    void emplace_back(T &&v, Args &&... args)
    {
        push_back(
            std::string_view(std::forward<T>(v), std::forward<Args>(args)...));
    }

    void emplace_back()
    {
        if (mask_.empty()) {
            mask_.resize(index_.size());
            std::fill(mask_.begin(), mask_.end(), true);
        }

        mask_.push_back(false);
        index_.push_back(-1);
    }

    void clear()
    {
        mask_.clear();
        pool_.clear();
        levels_.clear();
        index_.clear();
    }

    std::string_view operator[](std::size_t i) const
    {
        auto idx = index_[i];

        if (idx < 0) {
            return {};
        }

        return levels_[static_cast<std::size_t>(idx)];
    }

    std::string_view at(std::size_t i) const
    {
        auto idx = index_.at(i);

        if (idx < 0) {
            return {};
        }

        return levels_.at(static_cast<std::size_t>(idx));
    }

    void reserve(std::size_t n) { index_.reserve(n); }

    std::shared_ptr<::arrow::Array> as_array() const
    {
        ::arrow::StringBuilder level_builder(::arrow::default_memory_pool());
        DF_ARROW_ERROR_HANDLER(level_builder.AppendValues(levels_));

        std::shared_ptr<::arrow::Array> level_array;
        DF_ARROW_ERROR_HANDLER(level_builder.Finish(&level_array));

        ::arrow::Int32Builder index_builder(::arrow::default_memory_pool());
        if (null_count() == 0) {
            DF_ARROW_ERROR_HANDLER(index_builder.AppendValues(index_));
        } else {
            DF_ARROW_ERROR_HANDLER(index_builder.AppendValues(index_, mask_));
        }

        std::shared_ptr<::arrow::Array> index_array;
        DF_ARROW_ERROR_HANDLER(index_builder.Finish(&index_array));

        std::shared_ptr<::arrow::Array> ret;
        DF_ARROW_ERROR_HANDLER(::arrow::DictionaryArray::FromArrays(
            std::make_shared<::arrow::DictionaryType>(
                ::arrow::int32(), level_array),
            index_array, &ret));

        return ret;
    }

  private:
    std::vector<std::int32_t> index_;
    std::vector<std::string> levels_;
    std::unordered_map<std::string_view, std::int32_t> pool_;
};

inline std::shared_ptr<::arrow::Array> make_array(
    const CategoricalArray &values)
{
    return values.as_array();
}

inline void cast_array(const ::arrow::Array &values, CategoricalArray *out)
{
    internal::CategoricalVisitor<CategoricalArray> visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CATEGORICAL_HPP
