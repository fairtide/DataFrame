# DataFrame in C++

# Introduction

```{.cpp}
::dataframe::DataFrame people(
    std::pair{"ID", std::vector{20, 40}},
    std::pair{"Name", std::vector{"John Doe", "Jane Doe"}} 
);

::dataframe::DataFrame jobs(
    std::pair{"ID", std::vector{20, 60}},
    std::pair{"Job", std::vector{"Lawyer", "Doctor"}}
);

// zero-copy view
people["ID"].view<int>()[1]; // 40

// cast
people["Name"].as<std::string>()[0]; // "John Doe"

// zero-copy view if possible, otherwise cast
people["Name"].as_view<std::string>()[1]; // "Jane Doe"

// add column
people["Gen"] = std::vector{"Male", "Female"};

// join
::dataframe::join(people, jobs, "ID", ::dataframe::JoinType::Inner, true)

// sort
::dataframe::sort(people, "Name");
::dataframe::sort(people, "Name", true); // reverse order

// bind rows
df1 = people
df2 = jobs
df1["Name"].remove();
df2["Job"].remove();
::dataframe::bind_rows({df1, df2});

// bind columns
jobs["ID"].remove();
::dataframe::bind_cols({people, job});
```

# LICENSE

License (See LICENSE file for full license)

Copyright 2019 Fairtide Pte. Ltd.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

[][http://www.apache.org/licenses/LICENSE-2.0]

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
