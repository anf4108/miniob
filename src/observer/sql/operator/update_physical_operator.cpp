#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "sql/expr/tuple.h"
#include "common/value.h"
#include <cstring>

RC UpdatePhysicalOperator::next() {
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  LOG_INFO("children[0]: %x",&children_[0]);
  return children_[0]->next();
}

RC UpdatePhysicalOperator::open(Trx *trx) {
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS)
    return rc;

  vector<RID> to_update;
  vector<vector<char>> records;
  
  // 1. 先收集所有需要更新的记录ID和内容
  while ((rc = children_[0]->next()) == RC::SUCCESS) {
    auto *tuple = static_cast<RowTuple *>(children_[0]->current_tuple());
    auto &record = tuple->record();
    
    // 保存记录ID和内容
    to_update.push_back(record.rid());
    vector<char> r(table_->table_meta().record_size());
    memcpy(r.data(), record.data(), r.size());
    records.push_back(r);
  }
  // 2. 释放所有读锁
  children_[0]->close();
  children_.clear();
  
  if (rc != RC::RECORD_EOF) {
    return rc;
  }
  // 3. 按固定顺序获取写锁并更新
  vector<RID> inserted;
  for (size_t i = 0; i < to_update.size(); i++) {
    Record record;
    rc = table_->visit_record(to_update[i], [&](Record &rec) {
      record = rec;
      return true;
    });
    // 删除原记录
    rc = table_->delete_record(record);
    // 插入更新后的记录
    RID rid;
    rc = update(records[i], rid);
    inserted.push_back(rid);
    // 回滚 暂无需求
    // if (rc != RC::SUCCESS) {
    //   // 回滚已更新的记录
    //   RC rc1 = remove_all(inserted);
    //   if (rc1 != RC::SUCCESS) {
    //     LOG_ERROR("failed to rollback update, error in remove inserted");
    //   }
    //   rc1 = insert_all(records);
    //   if (rc1 != RC::SUCCESS) {
    //     LOG_ERROR("failed to rollback update, error in insert deleted");
    //   }
    //   return rc;
    // }
  }
  
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::insert_all(vector<vector<char>> &v) {
  RC rc_ret = RC::SUCCESS;
  for (auto &x : v) {
    RID rid;
    RC rc = insert(x, rid);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("fail to insert all");
      if (rc_ret == RC::SUCCESS) {
        rc_ret = rc;
      }
    }
  }
  return rc_ret;
}

RC UpdatePhysicalOperator::insert(vector<char> &v, RID &rid) {
  Record record;
  RC rc = table_->make_record(v.data(), v.size(), record);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("fail to make record");
    return rc;
  }
  rc = table_->insert_record(record);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("fail to insert record");
    return rc;
  }
  rid = record.rid();
  return rc;
}

RC UpdatePhysicalOperator::remove_all(const vector<RID> &rids) {
  RC rc_ret = RC::SUCCESS;
  for (auto &rid : rids) {
    RC rc = table_->delete_record(rid);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("fail to delete record");
      if (rc_ret == RC::SUCCESS) {
        rc_ret = rc;
      }
    }
  }
  return rc_ret;
}

RC UpdatePhysicalOperator::update(vector<char> v, RID &rid) {
  const auto *meta = update_field_.meta();
  int offset = meta->offset();
  int field_len = meta->len();
  
  // 检查边界
  if (offset < 0 || offset + field_len > v.size()) {
    LOG_ERROR("Invalid field offset or length. offset=%d, len=%d, record_size=%zu", 
              offset, field_len, v.size());
    return RC::INVALID_ARGUMENT;
  }
  
  int len = min(field_len, value_.length());
  
  memcpy(v.data() + offset, value_.data(), len);
  return insert(v, rid);
}

RC UpdatePhysicalOperator::close() {
  return RC::SUCCESS;
}