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

#include <dataframe/array/view.hpp>

#include <catch2/catch.hpp>

TEMPLATE_TEST_CASE("Make view of array/slice", "[make_view]", std::uint8_t,
    std::int8_t, std::uint16_t, std::int16_t, std::uint32_t, std::int32_t,
    std::uint64_t, std::int64_t, float, double)
{
    std::size_t n = 1000;
    std::ptrdiff_t m = 500;
    std::mt19937_64 rng;
    std::bernoulli_distribution rbit;
    std::vector<bool> valids;
    std::vector<TestType> values;

    using traits = ::dataframe::TypeTraits<TestType>;

    for (std::size_t i = 0; i != n; ++i) {
        auto x = rbit(rng);
        auto y = static_cast<TestType>(i);
        valids.push_back(x);
        values.push_back(y);
    }

    SECTION("View of array")
    {
        auto builder = traits::builder();
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(values));

        std::shared_ptr<::arrow::Array> array;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&array));

        auto view = ::dataframe::make_view<TestType>(array);

        CHECK(view.size() == n);

        CHECK(view.array()->null_count() == 0);

        CHECK(std::equal(
            view.begin(), view.end(), values.begin(), values.end()));
    }

    SECTION("View of slice")
    {
        auto builder = traits::builder();
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(values));

        std::shared_ptr<::arrow::Array> array;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&array));
        array = array->Slice(static_cast<std::int64_t>(m));

        auto view = ::dataframe::make_view<TestType>(array);

        CHECK(view.size() == n - static_cast<std::size_t>(m));

        CHECK(view.array()->null_count() == 0);

        CHECK(std::equal(
            view.begin(), view.end(), values.begin() + m, values.end()));
    }

    SECTION("View of nullable array")
    {
        auto builder = traits::builder();
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(values, valids));

        std::shared_ptr<::arrow::Array> array;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&array));

        auto view = ::dataframe::make_view<TestType>(array);

        CHECK(view.size() == n);

        CHECK(view.array()->null_count() ==
            array->length() -
                std::accumulate(valids.begin(), valids.end(), INT64_C(0)));

        CHECK(std::equal(
            view.begin(), view.end(), values.begin(), values.end()));
    }

    SECTION("View of nullable slice")
    {
        auto builder = traits::builder();
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(values, valids));

        std::shared_ptr<::arrow::Array> array;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&array));
        array = array->Slice(static_cast<std::int64_t>(m));

        auto view = ::dataframe::make_view<TestType>(array);

        CHECK(view.size() == n - static_cast<std::size_t>(m));

        CHECK(view.array()->null_count() ==
            array->length() -
                std::accumulate(valids.begin() + m, valids.end(), INT64_C(0)));

        CHECK(std::equal(
            view.begin(), view.end(), values.begin() + m, values.end()));
    }
}
