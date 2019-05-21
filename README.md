# DataFrame in C++

# Introduction

```cpp
#define RAPIDJSON_HAS_STDSTRING 1

#include <dataframe/dataframe.hpp>
#include <dataframe/serializer/json.hpp>

int main()
{
    ::dataframe::JSONColumnWriter data_writer;

    ::dataframe::DataFrame people;
    people["ID"] = std::vector{20, 40};
    people["Name"] = std::vector<std::string>{"John Doe", "Jane Doe"};

    data_writer.write(people);
    std::cout << "people: " << data_writer.str() << std::endl;

    ::dataframe::DataFrame jobs;
    jobs["ID"] = std::vector{20, 60};
    jobs["Job"] = std::vector<std::string>{"Lawyer", "Doctor"};

    data_writer.write(jobs);
    std::cout << "jobs: " << data_writer.str() << std::endl;

    // zero-copy view
    people["ID"].view<int>()[1]; // 40

    // zero-copy view if possible, otherwise cast
    people["Name"].as<std::string>()[0]; // "John Doe"

    // join
    auto df = ::dataframe::join(
        people, jobs, "ID", ::dataframe::JoinType::Inner, true);

    data_writer.write(df);
    std::cout << "join: " << data_writer.str() << std::endl;

    // sort
    df = ::dataframe::sort(people, "Name");
    data_writer.write(df);
    std::cout << "sort " << data_writer.str() << std::endl;

    // reverse sort
    df = ::dataframe::sort(people, "Name", true);
    data_writer.write(df);
    std::cout << "sort: " << data_writer.str() << std::endl;

    // bind rows
    auto df1 = people;
    auto df2 = jobs;
    df1["Name"].remove();
    df2["Job"].remove();
    df = ::dataframe::bind_rows({df1, df2});

    data_writer.write(df);
    std::cout << "bind_rows: " << data_writer.str() << std::endl;

    // bind columns
    jobs["ID"].remove();
    df = ::dataframe::bind_cols({people, jobs});

    data_writer.write(df);
    std::cout << "bind_cols: " << data_writer.str() << std::endl;
}
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
