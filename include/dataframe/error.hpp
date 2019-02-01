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

#ifndef DATAFRAME_INTERNAL_ERROR_HPP
#define DATAFRAME_INTERNAL_ERROR_HPP

#include <arrow/api.h>
#include <sstream>
#include <stdexcept>

#define DF_ARROW_ERROR_HANDLER(call)                                          \
    {                                                                         \
        ::dataframe::ErrorHandler(call, __FILE__, __LINE__, #call);           \
    }

namespace dataframe {

class DataFrameException : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

inline void ErrorHandler(const ::arrow::Status &status, const char *file,
    int line, const char *call)
{
    if (status.ok()) {
        return;
    }

    std::stringstream ss;
    ss << file << ":" << line << ":" << call << ":" << status.ToString();

    throw DataFrameException(ss.str());
}

} // namespace dataframe

#endif // DATAFRAME_INTERNAL_ERROR_HPP
