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

#include <dataframe/table/data_frame.hpp>

#include <catch2/catch.hpp>

TEST_CASE("Copy on write", "[table]")
{
    ::dataframe::DataFrame people;
    people["ID"] = std::vector<int>{20, 40};
    people["Name"] = std::vector<std::string>{"John Doe", "Jane Doe"};

    ::dataframe::DataFrame other = people;

    CHECK(&other.table() == &people.table());

    SECTION("Muable column")
    {
        people["ID"] = ::dataframe::repeat(30);
        CHECK(&other.table() != &people.table());
        CHECK(people["ID"].view<int>().front() == 30);
        CHECK(people["ID"].view<int>().back() == 30);
        CHECK(other["ID"].view<int>().front() == 20);
        CHECK(other["ID"].view<int>().back() == 40);
    }

    SECTION("Add column")
    {
        people["Count"] = ::dataframe::repeat(30);
        CHECK(&other.table() != &people.table());
        CHECK(people.ncol() == 3);
        CHECK(other.ncol() == 2);
    }

    SECTION("Remove column")
    {
        people["ID"].remove();
        CHECK(&other.table() != &people.table());
        CHECK(people.ncol() == 1);
        CHECK(other.ncol() == 2);
    }

    SECTION("Rename column")
    {
        people["ID"].rename("Count");
        CHECK(&other.table() != &people.table());
        CHECK(people[0].name() == "Count");
        CHECK(other[0].name() == "ID");
    }
}
