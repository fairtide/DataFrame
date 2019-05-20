//============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//============================================================================

#ifndef DATAFRAME_SERIALIZER_BSON_COLUMN_READER_HPP
#define DATAFRAME_SERIALIZER_BSON_COLUMN_READER_HPP

#include <dataframe/serializer/bson/data_reader.hpp>
#include <dataframe/serializer/bson/type_reader.hpp>

namespace dataframe {

namespace bson {

class ColumnReader
{
  public:
    ColumnReader(::arrow::MemoryPool *pool)
        : pool_(pool)
    {
    }

    std::shared_ptr<::arrow::Array> read(const ::bsoncxx::document::view &view)
    {
        ::arrow::ArrayData data(nullptr, 0);
        data.type = read_type(view);
        DataReader reader(data, view, pool_);
        DF_ARROW_ERROR_HANDLER(data.type->Accept(&reader));

        return ::arrow::MakeArray(
            std::make_shared<::arrow::ArrayData>(std::move(data)));
    }

  private:
    ::arrow::MemoryPool *pool_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_COLUMN_READER_HPP
