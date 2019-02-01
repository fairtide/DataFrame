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

#include <iostream>
#include <random>

#include <dataframe/dataframe.hpp>

int main()
{
    using ::dataframe::ArrayView;
    using ::dataframe::CategoricalArray;
    using ::dataframe::DataFrame;

    DataFrame df;

    std::size_t n = 1000;
    std::mt19937_64 rng;
    std::uniform_real_distribution<double> runif(0, 1);

    std::vector<double> x(n);
    for (std::size_t i = 0; i != n; ++i) {
        x[i] = runif(rng);
    }

    CategoricalArray v;
    for (std::size_t i = 0; i != n; ++i) {
        v.emplace_back(std::to_string(i % 10));
    }

    df["x"] = x;
    df["y"] = 2;
    df["z"] = INT64_C(3);
    df["u"] = "abc";
    df["v"] = v;

    if (df["z"].is_ctype<std::int64_t>()) {
        auto z = df["z"].view<std::int64_t>();
        std::cout << "z: view int64 : " << z.front() << ", ..., " << z.back()
                  << std::endl;
    }

    if (df["z"].is_convertible<int>()) {
        auto z = df["z"].as<int>();
        std::cout << "z: as int : " << z.front() << ", " << z.size()
                  << std::endl;
    }

    if (df["u"].is_convertible<std::string_view>()) {
        auto u = df["u"].as<std::string_view>();
        std::cout << "u: as string_view : " << u.front() << ", " << u.size()
                  << std::endl;
    } else {
        throw std::runtime_error(
            "Expected column z to be strings or categoricals");
    }

    df["v"].as<CategoricalArray>();
    df["v"].as<std::string>();

    auto slice = df["x"](2, 10);
    std::cout << slice.array().length() << std::endl;
    std::cout << slice.array().type()->name() << std::endl;
    auto y = slice.view<double>();
    std::cout << x[2] << ", " << x[9] << std::endl;
    std::cout << y.front() << ", " << y.back() << std::endl;

    df.feather_write("foo.cpp");
    df.feather_read("foo.cpp");

    auto buf = df.write<::dataframe::DataFormat::RecordBatchStream>();
    df.read<::dataframe::DataFormat::RecordBatchStream>(*buf);

    return 0;
}
