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

#include <dataframe/dataframe.hpp>
#include <gtest/gtest.h>

using View = ::dataframe::ArrayView<const char *, std::vector<const char *>>;
using Mask = ::dataframe::ArrayMask;

TEST(Join, Inner)
{
    ::dataframe::DataFrame people(                             //
        std::pair{"ID", std::vector{20, 40}},                  //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} //
    );

    ::dataframe::DataFrame jobs(                          //
        std::pair{"ID", std::vector{20, 60}},             //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    ::dataframe::DataFrame ret(        //
        std::pair{"ID", 20},           //
        std::pair{"Name", "John Doe"}, //
        std::pair{"Job", "Lawyer"}     //
    );

    EXPECT_EQ(::dataframe::join(
                  people, jobs, "ID", ::dataframe::JoinType::Inner, true),
        ret);
}

TEST(Join, Outer)
{
    ::dataframe::DataFrame people(                             //
        std::pair{"ID", std::vector{20, 40}},                  //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} //
    );

    ::dataframe::DataFrame jobs(                          //
        std::pair{"ID", std::vector{20, 60}},             //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    ::dataframe::DataFrame ret(                   //
        std::pair{"ID", std::vector{20, 40, 60}}, //
        std::pair{"Name",
            View{std::vector{"John Doe", "Jane Doe", ""},
                Mask{std::vector{true, true, false}}}}, //
        std::pair{"Job",
            View{std::vector{"Lawyer", "", "Doctor"},
                Mask{std::vector{true, false, true}}}} //
    );

    EXPECT_EQ(::dataframe::join(
                  people, jobs, "ID", ::dataframe::JoinType::Outer, true),
        ret);
}

TEST(Join, Left)
{
    ::dataframe::DataFrame people(                             //
        std::pair{"ID", std::vector{20, 40}},                  //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} //
    );

    ::dataframe::DataFrame jobs(                          //
        std::pair{"ID", std::vector{20, 60}},             //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    ::dataframe::DataFrame ret(                                 //
        std::pair{"ID", std::vector{20, 40}},                   //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}}, //
        std::pair{"Job",
            View{std::vector{"Lawyer", ""}, Mask{std::vector{true, false}}}} //
    );

    EXPECT_EQ(::dataframe::join(
                  people, jobs, "ID", ::dataframe::JoinType::Left, true),
        ret);
}

TEST(Join, Right)
{
    ::dataframe::DataFrame people(                             //
        std::pair{"ID", std::vector{20, 40}},                  //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} //
    );

    ::dataframe::DataFrame jobs(                          //
        std::pair{"ID", std::vector{20, 60}},             //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    ::dataframe::DataFrame ret(               //
        std::pair{"ID", std::vector{20, 60}}, //
        std::pair{"Name",
            View{std::vector{"John Doe", ""},
                Mask{std::vector{true, false}}}},         //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    EXPECT_EQ(::dataframe::join(
                  people, jobs, "ID", ::dataframe::JoinType::Right, true),
        ret);
}

TEST(Join, Semi)
{
    ::dataframe::DataFrame people(                             //
        std::pair{"ID", std::vector{20, 40}},                  //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} //
    );

    ::dataframe::DataFrame jobs(                          //
        std::pair{"ID", std::vector{20, 60}},             //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    ::dataframe::DataFrame ret(       //
        std::pair{"ID", 20},          //
        std::pair{"Name", "John Doe"} //
    );

    EXPECT_EQ(::dataframe::join(
                  people, jobs, "ID", ::dataframe::JoinType::Semi, true),
        ret);
}

TEST(Join, Anti)
{
    ::dataframe::DataFrame people(                             //
        std::pair{"ID", std::vector{20, 40}},                  //
        std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} //
    );

    ::dataframe::DataFrame jobs(                          //
        std::pair{"ID", std::vector{20, 60}},             //
        std::pair{"Job", std::vector{"Lawyer", "Doctor"}} //
    );

    ::dataframe::DataFrame ret(       //
        std::pair{"ID", 40},          //
        std::pair{"Name", "Jane Doe"} //
    );

    EXPECT_EQ(::dataframe::join(
                  people, jobs, "ID", ::dataframe::JoinType::Anti, true),
        ret);
}
