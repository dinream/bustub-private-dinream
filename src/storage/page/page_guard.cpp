#include "storage/page/page_guard.h"
#include <cstddef>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
    // page_=nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // Drop();
  this->guard_.page_ = that.guard_.page_;
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
    this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(), guard_.is_dirty_);
    this->guard_.bpm_ = nullptr;
    this->guard_.page_ = nullptr;
    this->guard_.is_dirty_ = false;
  }
}

ReadPageGuard::~ReadPageGuard() {
  if (this->guard_.bpm_ != nullptr && this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
    this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(), guard_.is_dirty_);
    this->guard_.bpm_ = nullptr;
    this->guard_.page_ = nullptr;
    this->guard_.is_dirty_ = false;
  }
  // 释放
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // 为什么 地址泄露 guard_ = std::move(that.guard_);
  this->guard_.page_ = that.guard_.page_;
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    this->guard_.page_ = that.guard_.page_;
    this->guard_.bpm_ = that.guard_.bpm_;
    this->guard_.is_dirty_ = that.guard_.is_dirty_;
    that.guard_.bpm_ = nullptr;
    that.guard_.page_ = nullptr;
    that.guard_.is_dirty_ = false;
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
    this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(), guard_.is_dirty_);
    this->guard_.bpm_ = nullptr;
    this->guard_.page_ = nullptr;
    this->guard_.is_dirty_ = false;
  }
}

WritePageGuard::~WritePageGuard() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
    this->guard_.bpm_->UnpinPage(this->guard_.page_->GetPageId(), guard_.is_dirty_);
    this->guard_.bpm_ = nullptr;
    this->guard_.page_ = nullptr;
    this->guard_.is_dirty_ = false;
  }
}  // NOLINT

}  // namespace bustub
