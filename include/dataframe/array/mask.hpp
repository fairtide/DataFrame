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
    ArrayMask()
        : size_(0)
        , null_count_(0)
    {
    }

    explicit ArrayMask(std::vector<bool> data)
        : data_(std::move(data))
        , size_(data_.size())
        , null_count_(0)
    {
        if (data_.empty()) {
            return;
        }

        for (auto v : data_) {
            null_count_ += !v;
        }
    }

    ArrayMask(std::size_t n, const uint8_t *bytes)
        : data_(n)
        , size_(n)
        , null_count_(0)
    {
        for (std::size_t i = 0; i != n; ++i) {
            auto v = static_cast<bool>(bytes[i / 8] & (1 << (i % 8)));
            data_[i] = v;
            null_count_ += !v;
        }
    }

    const std::vector<bool> &data() const { return data_; }

    std::size_t size() const { return size_; }

    std::size_t null_count() const { return null_count_; }

    bool operator[](std::size_t i) const
    {
        return null_count_ == 0 || data_[i];
    }

    bool at(std::size_t i) const { return null_count_ == 0 || data_.at(i); }

    void push_back(bool flag)
    {
        ++size_;
        null_count_ += !flag;
        if (data_.empty()) {
            if (flag) {
                return;
            } else {
                data_.resize(size_, true);
                data_.push_back(flag);
            }
        } else {
            data_.push_back(flag);
        }
    }

    void clear()
    {
        data_.clear();
        size_ = 0;
        null_count_ = 0;
    }

  protected:
    std::vector<bool> data_;
    std::size_t size_;
    std::size_t null_count_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MASK_HPP
