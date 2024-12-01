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

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {}

auto LRUKReplacer::Victim(frame_id_t* frame_id) -> bool {
    //WSDB_STUDENT_TODO(labs(), f1); 
    std::lock_guard<std::mutex> lock(latch_);
    frame_id_t v_frame_id = INVALID_FRAME_ID;
    LRUKNode* max_node;
    unsigned long long max_bkd = 0;
    for (auto& it : node_store_) {
        LRUKNode& node = it.second;
        if (node.IsEvictable()) {
            unsigned long long bkd = node.GetBackwardKDistance(cur_ts_);
            if (bkd > max_bkd) {
                max_bkd = bkd;
                max_node = &node;
                v_frame_id = it.first;
            }
            else if (max_bkd == inf && bkd == inf) {
                if (max_node->GetFirstTime() > node.GetFirstTime()) {
                    max_bkd = bkd;
                    max_node = &node;
                    v_frame_id = it.first;
                }
            }
        }
    }
    if (v_frame_id == INVALID_FRAME_ID)
        return false;
    else {
        *frame_id = v_frame_id;
        node_store_.erase(v_frame_id);
        --cur_size_;
        return true;
    }
}

void LRUKReplacer::Pin(frame_id_t frame_id) {
    //WSDB_STUDENT_TODO(l1, f1); 
    std::lock_guard<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end())
        node_store_[frame_id] = LRUKNode(frame_id, k_);
    LRUKNode& node = node_store_[frame_id];
    if (node.IsEvictable()) {
        node.SetEvictable(false);
        --cur_size_;
    }
    node.AddHistory(cur_ts_++);
}

void LRUKReplacer::Unpin(frame_id_t frame_id) {
    //WSDB_STUDENT_TODO(l1, f1); 
    std::lock_guard<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it != node_store_.end()) {
        it->second.SetEvictable(true);
        ++cur_size_;
    }
}

auto LRUKReplacer::Size() -> size_t {
    //WSDB_STUDENT_TODO(l1, f1); 
    std::lock_guard<std::mutex> lock(latch_);
    return cur_size_;
}

}  // namespace wsdb