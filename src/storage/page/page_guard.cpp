#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
}

void BasicPageGuard::Drop() { bpm_->UnpinPage(page_->GetPageId(), is_dirty_); }
// 移动赋值构造函数：记得要先将自己Drop 释放出锁 和pin_coun
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { bpm_->UnpinPage(page_->GetPageId(), is_dirty_); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_.page_ = that.guard_.page_;
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.page_->RLatch();
  return *this;
}

void ReadPageGuard::Drop() { guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_); }

ReadPageGuard::~ReadPageGuard() {
  this->guard_.page_->RUnlatch();
  // 释放
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_.page_ = that.guard_.page_;
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.page_->WLatch();
  return *this;
}

void WritePageGuard::Drop() {
  guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_); 
}

WritePageGuard::~WritePageGuard() {
  guard_.page_->WUnlatch();
  // 释放锁
}  // NOLINT

}  // namespace bustub
