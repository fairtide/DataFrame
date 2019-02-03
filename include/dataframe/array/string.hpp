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

#ifndef DATAFRAME_ARRAY_STRING_HPP
#define DATAFRAME_ARRAY_STRING_HPP

#include <dataframe/array/categorical.hpp>
#include <arrow/api.h>

namespace dataframe {

namespace internal {

template <typename T>
class StringVisitor : public ::arrow::ArrayVisitor
{
  public:
    StringVisitor(T *out)
        : out_(out)
    {
    }

    ::arrow::Status Visit(const ::arrow::BinaryArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) final
    {
        CategoricalVisitor<T> visitor(out_);
        return array.Accept(&visitor);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        auto n = array.length();
        out_->clear();
        out_->reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            if (array.IsValid(i)) {
                out_->emplace_back(array.GetView(i));
            } else {
                out_->emplace_back();
            }
        }

        return ::arrow::Status::OK();
    }

  private:
    T *out_;
};

} // namespace internal

template <typename T>
std::enable_if_t<std::is_constructible_v<std::string_view, T>,
    std::shared_ptr<::arrow::Array>>
make_array(const ArrayViewBase<T> &view)
{
    if (view.data() == nullptr) {
        return nullptr;
    }

    ::arrow::StringBuilder builder(::arrow::default_memory_pool());
    auto n = view.size();
    auto v = view.data();
    for (std::size_t i = 0; i != n; ++i) {
        DF_ARROW_ERROR_HANDLER(builder.Append(std::string_view(v[i])));
    }

    std::shared_ptr<::arrow::Array> out;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&out));

    return out;
}

template <typename T, typename Alloc>
inline std::enable_if_t<std::is_constructible_v<T, std::string_view>>
cast_array(const ::arrow::Array &values, std::vector<T, Alloc> *out)
{
    internal::StringVisitor<std::vector<T, Alloc>> visitor(out);
    DF_ARROW_ERROR_HANDLER(values.Accept(&visitor));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_STRING_HPP
