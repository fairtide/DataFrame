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

#include <dataframe/array/list.hpp>

#include <catch2/catch.hpp>

TEST_CASE("Make view of list array/slice", "[make_view]")
{
    using V = std::int32_t;
    using traits = ::dataframe::TypeTraits<V>;
    using T = ::dataframe::ListView<V>;

    std::size_t n = 1000;
    // std::ptrdiff_t m = 500;
    std::mt19937_64 rng;
    std::bernoulli_distribution rbit;
    std::vector<bool> valids;
    std::vector<std::vector<V>> values;

    for (std::size_t i = 0; i != n; ++i) {
        auto x = rbit(rng);
        std::vector<V> y(i);
        for (auto &v : y) {
            v = static_cast<V>(i);
        }
        valids.push_back(x);
        values.push_back(std::move(y));
    }

    auto make_array = [&](bool nullable) {
        auto values_builder = traits::builder();
        auto offsets_builder = traits::builder();

        std::size_t offset = 0;
        for (auto &v : values) {
            DF_ARROW_ERROR_HANDLER(values_builder->AppendValues(v));
            if (!valids[v.size()] && nullable) {
                DF_ARROW_ERROR_HANDLER(offsets_builder->AppendNull());
            } else {
                DF_ARROW_ERROR_HANDLER(
                    offsets_builder->Append(static_cast<V>(offset)));
            }
            offset += v.size();
        }
        DF_ARROW_ERROR_HANDLER(
            offsets_builder->Append(static_cast<V>(offset)));

        std::shared_ptr<::arrow::Array> values_array;
        std::shared_ptr<::arrow::Array> offsets_array;

        DF_ARROW_ERROR_HANDLER(values_builder->Finish(&values_array));
        DF_ARROW_ERROR_HANDLER(offsets_builder->Finish(&offsets_array));

        std::shared_ptr<::arrow::Array> ret;
        DF_ARROW_ERROR_HANDLER(::arrow::ListArray::FromArrays(*offsets_array,
            *values_array, ::arrow::default_memory_pool(), &ret));

        return ret;
    };

    SECTION("View of array")
    {
        auto array = make_array(false);
        auto view = ::dataframe::make_view<T>(array);

        CHECK(view.size() == n);

        CHECK(view.data()->null_count() == 0);

        CHECK(std::equal(view.begin(), view.end(), values.begin(),
            values.end(), [](auto &&v1, auto &&v2) {
                return std::equal(v1.begin(), v1.end(), v2.begin(), v2.end());
            }));
    }

    // SECTION("View of slice")
    // {
    // }

    // SECTION("View of nullable array")
    // {
    // }

    // SECTION("View of nullable slice")
    // {
    // }
}
