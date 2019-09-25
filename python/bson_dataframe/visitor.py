# ============================================================================
# Copyright 2019 Fairtide Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================

class Visitor():
    def visit_null(self, obj):
        raise NotImplementedError()

    def visit_numeric(self, obj):
        raise NotImplementedError()

    def visit_bool(self, obj):
        return self.visit_numeric(obj)

    def visit_int8(self, obj):
        return self.visit_numeric(obj)

    def visit_int16(self, obj):
        return self.visit_numeric(obj)

    def visit_int32(self, obj):
        return self.visit_numeric(obj)

    def visit_int64(self, obj):
        return self.visit_numeric(obj)

    def visit_uint8(self, obj):
        return self.visit_numeric(obj)

    def visit_uint16(self, obj):
        return self.visit_numeric(obj)

    def visit_uint32(self, obj):
        return self.visit_numeric(obj)

    def visit_uint64(self, obj):
        return self.visit_numeric(obj)

    def visit_float16(self, obj):
        return self.visit_numeric(obj)

    def visit_float32(self, obj):
        return self.visit_numeric(obj)

    def visit_float64(self, obj):
        return self.visit_numeric(obj)

    def visit_date(self, obj):
        return self.visit_datetime(obj)

    def visit_timestamp(self, obj):
        return self.visit_datetime(obj)

    def visit_time(self, obj):
        return self.visit_datetime(obj)

    def visit_opaque(self, obj):
        raise NotImplementedError()

    def visit_binary(self, obj):
        raise NotImplementedError()

    def visit_bytes(self, obj):
        return self.visit_binary(obj)

    def visit_utf8(self, obj):
        return self.visit_binary(obj)

    def visit_dictionary(self, obj):
        raise NotImplementedError()

    def visit_ordered(self, obj):
        return self.visit_dictionary(obj)

    def visit_factor(self, obj):
        return self.visit_dictionary(obj)

    def visit_list(self, obj):
        raise NotImplementedError()

    def visit_struct(self, obj):
        raise NotImplementedError()
