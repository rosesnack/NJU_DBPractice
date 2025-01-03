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

#include "executor_filter.h"

namespace wsdb {

FilterExecutor::FilterExecutor(AbstractExecutorUptr child, std::function<bool(const Record &)> filter)
    : AbstractExecutor(Basic), child_(std::move(child)), filter_(std::move(filter))
{}
void FilterExecutor::Init() { 
    //WSDB_STUDENT_TODO(l2, t1); 
    child_->Init();
    record_ = child_->GetRecord();
    while (!IsEnd() && !filter_(*record_)) {
        child_->Next();
        record_ = child_->GetRecord();
    }
}

void FilterExecutor::Next() { 
    //WSDB_STUDENT_TODO(l2, t1); 
    if (IsEnd()) {
      WSDB_FETAL("FilterExecutor is end");
    }
    child_->Next();
    record_ = child_->GetRecord();
    while (!IsEnd() && !filter_(*record_)) {
        child_->Next();
        record_ = child_->GetRecord();
    }
}

auto FilterExecutor::IsEnd() const -> bool { 
    //WSDB_STUDENT_TODO(l2, t1); 
    return child_->IsEnd();
}

auto FilterExecutor::GetOutSchema() const -> const RecordSchema * { return child_->GetOutSchema(); }
}  // namespace wsdb
