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

#ifndef DATAFRAME_ARRAY_MASK_HPP
#define DATAFRAME_ARRAY_MASK_HPP

#include <vector>

namespace dataframe {

class ArrayMask
{
  public:
    ArrayMask() = default;

    explicit ArrayMask(std::vector<bool> mask)
        : mask_(std::move(mask))
    {
    }

    const std::vector<bool> mask() const { return mask_; }

    std::size_t null_count() const
    {
        if (mask_.empty()) {
            return 0;
        }

        std::size_t ret = 0;
        for (auto v : mask_) {
            ret += !v;
        }

        return ret;
    }

    bool operator[](std::size_t i) const
    {
        return mask_.empty() || mask_.at(i);
    }

  protected:
    std::vector<bool> mask_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MASK_HPP
