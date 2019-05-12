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

#include <dataframe/table/join.hpp>

#include <catch2/catch.hpp>

TEST_CASE("DataFrame Join", "[join]")
{
    ::dataframe::DataFrame people;
    people["ID"] = std::vector<int>{20, 40};
    people["Name"] = std::vector<std::string>{"John Doe", "Jane Doe"};

    ::dataframe::DataFrame jobs;
    jobs["ID"] = std::vector<int>{20, 60};
    jobs["Job"] = std::vector<std::string>{"Lawyer", "Doctor"};

    SECTION("Inner")
    {
        ::dataframe::DataFrame ret;
        ret["ID"] = ::dataframe::repeat(20, 1);
        ret["Name"] = ::dataframe::repeat(std::string("John Doe"));
        ret["Job"] = ::dataframe::repeat(std::string("Lawyer"));

        CHECK(::dataframe::join(people, jobs, "ID",
                  ::dataframe::JoinType::Inner, true) == ret);
    }

    SECTION("Outer")
    {
        ::dataframe::DataFrame ret;
        ret["ID"] = std::vector<int>{20, 40, 60};
        ret["Name"].emplace<std::string>(
            std::vector{"John Doe", "Jane Doe", ""},
            std::vector{true, true, false});
        ret["Job"].emplace<std::string>(std::vector{"Lawyer", "", "Doctor"},
            std::vector{true, false, true});

        CHECK(::dataframe::join(people, jobs, "ID",
                  ::dataframe::JoinType::Outer, true) == ret);
    }

    SECTION("Left")
    {
        ::dataframe::DataFrame ret;
        ret["ID"] = std::vector{20, 40};
        ret["Name"] = std::vector<std::string>{"John Doe", "Jane Doe"};
        ret["Job"].emplace<std::string>(
            std::vector{"Lawyer", ""}, std::vector{true, false});

        CHECK(::dataframe::join(people, jobs, "ID",
                  ::dataframe::JoinType::Left, true) == ret);
    }

    SECTION("Right")
    {
        ::dataframe::DataFrame ret;
        ret["ID"] = std::vector{20, 60};
        ret["Name"].emplace<std::string>(
            std::vector{"John Doe", ""}, std::vector{true, false});
        ret["Job"] = std::vector<std::string>{"Lawyer", "Doctor"};

        CHECK(::dataframe::join(people, jobs, "ID",
                  ::dataframe::JoinType::Right, true) == ret);
    }

    SECTION("Semi")
    {
        ::dataframe::DataFrame ret;
        ret["ID"] = ::dataframe::repeat(20, 1);
        ret["Name"] = ::dataframe::repeat(std::string("John Doe"), 1);

        CHECK(::dataframe::join(people, jobs, "ID",
                  ::dataframe::JoinType::Semi, true) == ret);
    }

    SECTION("Anti")
    {
        ::dataframe::DataFrame ret;
        ret["ID"] = ::dataframe::repeat(40, 1);
        ret["Name"] = ::dataframe::repeat(std::string("Jane Doe"), 1);

        CHECK(::dataframe::join(people, jobs, "ID",
                  ::dataframe::JoinType::Anti, true) == ret);
    }
}
