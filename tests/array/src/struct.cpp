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

#include <dataframe/array/struct.hpp>

#include <catch2/catch.hpp>

TEST_CASE("Make view of struct array/slice", "[make_view]")
{
    using T1 = std::int32_t;
    using T2 = std::int64_t;
    using traits1 = ::dataframe::TypeTraits<T1>;
    using traits2 = ::dataframe::TypeTraits<T2>;
    using T = ::dataframe::Struct<T1, T2>;

    std::size_t n = 1000;
    // std::ptrdiff_t m = 500;
    std::mt19937_64 rng;
    std::bernoulli_distribution rbit;
    std::vector<bool> valids;
    std::vector<T1> values1;
    std::vector<T2> values2;
    std::vector<T> values;

    for (std::size_t i = 0; i != n; ++i) {
        auto x = rbit(rng);
        auto y = static_cast<T1>(i);
        auto z = static_cast<T2>(i * 2);
        valids.push_back(x);
        values1.push_back(y);
        values2.push_back(z);
        values.emplace_back(y, z);
    }

    auto make_array = [&](bool nullable) {
        auto builder1 = traits1::builder();
        auto builder2 = traits2::builder();

        DF_ARROW_ERROR_HANDLER(builder1->AppendValues(values1));
        DF_ARROW_ERROR_HANDLER(builder2->AppendValues(values2));

        std::shared_ptr<::arrow::Array> array1;
        std::shared_ptr<::arrow::Array> array2;

        DF_ARROW_ERROR_HANDLER(builder1->Finish(&array1));
        DF_ARROW_ERROR_HANDLER(builder2->Finish(&array2));

        std::vector<std::shared_ptr<::arrow::Field>> fields;
        fields.push_back(
            ::arrow::field(array1->type()->name(), array1->type()));
        fields.push_back(
            ::arrow::field(array2->type()->name(), array2->type()));

        auto type = ::arrow::struct_(fields);

        std::vector<std::shared_ptr<::arrow::Array>> children;
        children.push_back(array1);
        children.push_back(array2);

        std::unique_ptr<::arrow::Buffer> nulls;
        DF_ARROW_ERROR_HANDLER(
            ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
                ::arrow::BitUtil::BytesForBits(static_cast<std::int64_t>(n)),
                &nulls));

        auto bits =
            dynamic_cast<::arrow::MutableBuffer &>(*nulls).mutable_data();

        for (std::size_t i = 0; i != n; ++i) {
            ::arrow::BitUtil::SetBitTo(
                bits, static_cast<std::int64_t>(i), valids[i]);
        }

        return nullable ?
            std::make_shared<::arrow::StructArray>(type,
                static_cast<std::int64_t>(n), children, std::move(nulls)) :
            std::make_shared<::arrow::StructArray>(
                type, static_cast<std::int64_t>(n), children);
    };

    SECTION("View of array")
    {
        auto array = make_array(false);
        auto view = ::dataframe::make_view<T>(array);

        CHECK(view.size() == n);

        CHECK(view.data()->null_count() == 0);

        CHECK(std::equal(
            view.begin(), view.end(), values.begin(), values.end()));
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
