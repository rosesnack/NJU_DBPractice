/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/18.
//

#include "executor_insert.h"

namespace wsdb {

InsertExecutor::InsertExecutor(TableHandle *tbl, std::list<IndexHandle *> indexes, std::vector<RecordUptr> inserts)
    : AbstractExecutor(DML), tbl_(tbl), indexes_(std::move(indexes)), inserts_(std::move(inserts)), is_end_(false)
{
  std::vector<RTField> fields(1);
  fields[0]   = RTField{.field_ = {.field_name_ = "inserted", .field_size_ = sizeof(int), .field_type_ = TYPE_INT}};
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void InsertExecutor::Init() { WSDB_FETAL("InsertExecutor does not support Init"); }

void InsertExecutor::Next()
{
  // number of inserted records
  int count = 0;
  // WSDB_STUDENT_TODO(l2, t1);
  if (is_end_) {
    WSDB_FETAL("InsertExecutor is end");
  }
  for (auto &rec : inserts_) {
    tbl_->InsertRecord(*rec);
    ++count;
  }

  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(count)};
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  is_end_ = true;
}

auto InsertExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace wsdb
