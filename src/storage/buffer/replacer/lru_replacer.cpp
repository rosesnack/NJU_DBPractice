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
// Created by ziqi on 2024/7/17.
//

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool  { 
    //WSDB_STUDENT_TODO(l1, t1); 
    std::lock_guard<std::mutex> lock(latch_);
    //使用反向迭代器从后往前遍历
    for (auto rit = lru_list_.rbegin(); rit != lru_list_.rend(); ++rit) {
        if (rit->second == true) { 
            frame_id_t fid = rit->first;
            *frame_id = fid;
            lru_list_.erase((++rit).base());  //rit.base()指向的是it视角下rit指向元素的下一个
            lru_hash_.erase(fid);
            --cur_size_; 
            return true;
        }
    }
    return false;
 }

void LRUReplacer::Pin(frame_id_t frame_id) {
    //WSDB_STUDENT_TODO(l1, t1); 
    std::lock_guard<std::mutex> lock(latch_); 
    auto it = lru_hash_.find(frame_id);
    if (it != lru_hash_.end()) {
        if (it->second->second == true) { 
            it->second->second = false;
            --cur_size_;
        }
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);  
    }
    else
        lru_list_.push_front({frame_id, false});
    lru_hash_[frame_id] = lru_list_.begin();
 }

void LRUReplacer::Unpin(frame_id_t frame_id) {
    //WSDB_STUDENT_TODO(l1, t1);  
    std::lock_guard<std::mutex> lock(latch_);
    auto it = lru_hash_.find(frame_id);
    if (it != lru_hash_.end()) {
        if (it->second->second == false) {
            it->second->second = true;
            ++cur_size_;
        }
    }
 }

auto LRUReplacer::Size() -> size_t {
    //WSDB_STUDENT_TODO(l1, t1); 
    std::lock_guard<std::mutex> lock(latch_);
    return cur_size_;
}

}  // namespace wsdb