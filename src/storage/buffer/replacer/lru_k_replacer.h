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

#ifndef WSDB_LRU_K_REPLACER_H
#define WSDB_LRU_K_REPLACER_H
#include <list>
#include <mutex>
#include <unordered_map>
#include "replacer.h"
#include "../common/error.h"
#define inf std::numeric_limits<size_t>::max()

namespace wsdb {

class LRUKReplacer : public Replacer
{
public:
  explicit LRUKReplacer(size_t k);

  ~LRUKReplacer() override = default;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

private:
  class LRUKNode
  {
  public:
    LRUKNode() = default;

    explicit LRUKNode(frame_id_t fid, size_t k) : fid_(fid), k(k), is_evictable_(false) {}

    void AddHistory(timestamp_t ts) { 
      //WSDB_STUDENT_TODO(l1, f1); 
      history_.push_back(ts);
      if (history_.size() > k)
        history_.pop_front(); 
    }

    /**
     * Get the distance between the current timestamp and the k-th timestamp in the history,
     * think: why return type is unsigned long long?
     * @param cur_ts
     * @return
     */
    auto GetBackwardKDistance(timestamp_t cur_ts) -> unsigned long long
    {
      //WSDB_STUDENT_TODO(l1, f1);
      if (history_.size() < k) 
        return inf;
      else
        return (cur_ts - history_.front());
    }

    auto GetFirstTime() -> unsigned long long
    {
      return history_.front();
    }

    [[nodiscard]] auto IsEvictable() const -> bool { 
      //WSDB_STUDENT_TODO(l1, f1); 
      return is_evictable_;
    }

    auto SetEvictable(bool set_evictable) -> void { 
      //WSDB_STUDENT_TODO(l1, f1); 
      is_evictable_ = set_evictable;
    }

  private:
    std::list<timestamp_t> history_;
    frame_id_t             fid_{INVALID_FRAME_ID};
    size_t                 k{};
    bool                   is_evictable_{0};
  };

private:
  std::unordered_map<frame_id_t, LRUKNode> node_store_;  // frame_id -> LRUKNode
  size_t                                   cur_ts_{0};
  size_t                                   cur_size_{0};  // number of evictable frames
  size_t                                   max_size_;     // maximum number of frames that can be stored
  size_t                                   k_;            // k for LRU-k
  std::mutex                               latch_;        // mutex for curr_size_, node_store_, and curr_timestamp_
};
}  // namespace wsdb

#endif  // WSDB_LRU_K_REPLACER_H
