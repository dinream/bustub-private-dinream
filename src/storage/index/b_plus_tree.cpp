#include <arpa/inet.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  std::cout << "leaf_max_size" << leaf_max_size << " internal_max_size" << internal_max_size << std::endl;
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 判断当前 b+ 树是否为空
  if (header_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  // TODO(unknown): 可以用  pageread guard
  auto head_guard = bpm_->FetchPageRead(header_page_id_);
  // if(rg.PageId()==nullptr){
  //   return true;
  // }
  auto head_page = head_guard.As<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  auto root_guard = bpm_->FetchPageRead(head_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  // auto root_page = rg.As<BPlusTreeInternalPage >();
  // InternalPage *root_page = reinterpret_cast<BPlusTreePage>(rg);
  if (root_page == nullptr) {
    return true;
  }
  return root_page->GetSize() == 0;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  std::cout << std::this_thread::get_id() << "GetValue " << key << std::endl;
  // Declaration of context instance.
  // 这个使用方式待考究
  // Context ctx;
  // (void)ctx;
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);

  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  int idx;
  auto cur_id = header_page->root_page_id_;
  if (cur_id == INVALID_PAGE_ID) {
    return false;
  }
  auto cur_page_guard = bpm_->FetchPageRead(cur_id);
  header_page_guard.Drop();  // 释放 头节点 的读锁
  auto cur_page = cur_page_guard.template As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto *inner_page = cur_page_guard.As<InternalPage>();  // 太 ！ 对 ！ 啦 ！
    // InternalPage* inner_page= reinterpret_cast<InternalPage *>(page); // 虽然这个可以转化， 但是我觉得用整个page
    // 转换不太对，应该用page的date来转换
    inner_page->SearchKey(key, comparator_, idx);  // 这里的idx 可能是 0但是没有影响
    cur_id = inner_page->ValueAt(idx);
    auto new_guard = bpm_->FetchPageRead(cur_id);
    cur_page_guard = std::move(new_guard);
    cur_page = cur_page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = cur_page_guard.As<LeafPage>();
  if (leaf_page->SearchKey(key, comparator_, idx)) {
    result->push_back(leaf_page->ValueAt(idx));  // 将找到的值放进返回的地方
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // std::cout << std::this_thread::get_id() << "Insert " << key << std::endl;
  // Declaration of context instance.
  // 从头 header_page_id_ 开始 遍历 ，找叶子节点，然后插入
  // 进行比较-》 叶子节点插
  // 找插入的位置
  // 遍历当前节点， 找到一个位置，，如果当前节点不是 叶子---则将此位置作为下一个 当前节点，，，如果是叶子，则找到了
  // 判断是否 合并 or 分裂
  // 循环判断，，当前节点插入之后大于 m-1 --》分裂--》将父亲节点作为当前节点
  // 插入之后小于 ？？？ 小于节点怎么判断
  Context ctx;
  (void)ctx;
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;  // 没有 header  表示没有 B+ tree
  }
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  int idx;
  ctx.root_page_id_ = header_page->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {  // 此时 B+ tree  为空
    page_id_t page_id = INVALID_PAGE_ID;
    auto gu = bpm_->NewPageGuarded(&page_id);
    if (page_id == INVALID_PAGE_ID) {
      return false;  // 没有空闲页面了,返回false
    }
    auto page_guard = bpm_->FetchPageWrite(page_id);
    auto leaf_page = page_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->InsertIndex(0, key, value);  // TODO(unknown): 插入 pair;
    header_page->root_page_id_ = page_id;   // 设置 head中 root 的值；
    // page->WLatch();
    return true;  // 插入新节点
  }
  auto cur_page_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  if (cur_page_guard.PageId() == INVALID_PAGE_ID) {
    // 如果 fetch page 失败,
    return false;
  }
  ctx.write_set_.emplace_back(std::move(cur_page_guard));
  auto cur_page = ctx.write_set_.rbegin()->template As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto inner_page = ctx.write_set_.rbegin()->As<InternalPage>();  // 太 ！ 对 ！ 啦 ！
    // InternalPage* inner_page= reinterpret_cast<InternalPage *>(page); // 虽然这个可以转化， 但是我觉得用整个page
    // 转换不太对，应该用page的date来转换

    if (inner_page->GetSize() < inner_page->GetMaxSize() - 1) {
      // 祖宗有父亲罩着很安全，不管了
      // 但是父亲不能释放。
      ctx.header_page_->Drop();  // 头节点先放了
      while (ctx.write_set_.size() != 1) {
        ctx.write_set_.begin()->Drop();
        ctx.write_set_.pop_front();
      }
    }
    inner_page->SearchKey(key, comparator_, idx);
    page_id_t cur_id = inner_page->ValueAt(idx);
    cur_page_guard = bpm_->FetchPageWrite(cur_id);  // 拿到孩子的写锁
    cur_page = cur_page_guard.As<BPlusTreePage>();
    if (cur_page_guard.PageId() == INVALID_PAGE_ID) {
      // 如果 fetch page 失败,
      return false;
    }
    ctx.write_set_.emplace_back(std::move(cur_page_guard));
  }
  auto leaf_page = ctx.write_set_.rbegin()->AsMut<LeafPage>();  // 将这个  data 转换为  leaf_page
  if (leaf_page->SearchKey(key, comparator_, idx)) {
    return false;
  }

  leaf_page->InsertIndex(idx, key, value);
  MappingType mp;
  page_id_t page_id = INVALID_PAGE_ID;
  if (leaf_page->GetSize() > leaf_page->GetMaxSize() - 1) {
    auto gu = bpm_->NewPageGuarded(&page_id);
    if (page_id == INVALID_PAGE_ID) {
      leaf_page->DeleteIndex(idx);
      return false;  // 没有空闲页面了,返回false，暂时忽略
    }
    auto page_guard = bpm_->FetchPageWrite(page_id);
    auto new_leaf_page = page_guard.AsMut<LeafPage>();
    new_leaf_page->Init(leaf_max_size_);
    mp = leaf_page->Divid2Other(*new_leaf_page);  // 分裂
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(page_id);
  } else {
    return true;
  }
  ctx.write_set_.pop_back();  // 把叶子节点取出来
  // cur_page_guard.Drop();  // 释放 leaf 的写锁
  std::pair<KeyType, page_id_t> mmp{mp.first, page_id};
  // InternalPage* inner_page;
  while (!ctx.write_set_.empty()) {  // 递归分裂
    // cur_page_guard=const_cast<WritePageGuard *>(ctx.write_set_.begin());
    auto inner_page = ctx.write_set_.rbegin()->AsMut<InternalPage>();
    inner_page->SearchKey(mmp.first, comparator_, idx);
    inner_page->InsertIndex(idx + 1, mmp.first, mmp.second);
    if (inner_page->GetSize() > inner_page->GetMaxSize()) {
      page_id = INVALID_PAGE_ID;
      auto gu = bpm_->NewPageGuarded(&page_id);
      if (page_id == INVALID_PAGE_ID) {
        inner_page->DeleteIndex(idx);  // TODO(unknown): 这里的修复需要递归
        return false;                  // 没有空闲页面了,返回false，暂时忽略
      }
      auto page_guard = bpm_->FetchPageWrite(page_id);
      auto new_inner_page = page_guard.AsMut<InternalPage>();
      new_inner_page->Init(internal_max_size_);
      // MappingType mpp<page_id>=inner_page->Divid2Other(*new_inner_page);错误，这里会出现mapping type不匹配的问题。
      mmp = inner_page->Divid2Other(*new_inner_page);
      mmp.second = page_id;
    } else {
      return true;  // 结束递归
    }
    ctx.write_set_.rbegin()->Drop();
    ctx.write_set_.pop_back();
  }
  // 写集合为空并且递归没有结束，此时，构建新的根节点
  page_id_t page_idd = INVALID_PAGE_ID;
  { bpm_->NewPageGuarded(&page_idd); }
  if (page_idd == INVALID_PAGE_ID) {
    return false;  // 没有空闲页面了,返回false
  }
  auto page_guard = bpm_->FetchPageWrite(page_idd);
  auto root_inner_page = page_guard.AsMut<InternalPage>();
  auto root_pg = bpm_->FetchPageWrite(ctx.root_page_id_);
  auto root_page = root_pg.AsMut<InternalPage>();
  root_inner_page->Init(internal_max_size_);
  root_inner_page->InsertIndex(0, root_page->KeyAt(0), ctx.root_page_id_);
  root_inner_page->InsertIndex(1, mmp.first, page_id);  // 新分裂出来的，id
  header_page->root_page_id_ = page_idd;                // 设置 head中 root 的值；
                                                        // page->WLatch();
  return true;                                          // 插入新节点
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // std::cout << std::this_thread::get_id() << "Remove " << key << std::endl;
  // Declaration of context instance.
  // 空树 立即返回。。

  // 从头 header_page_id_ 开始 遍历 ，找叶子节点，然后删除
  // 找插入的位置
  // 遍历当前节点， 找到一个位置，，如果当前节点不是 叶子---则将此位置作为下一个 当前节点，，，如果是叶子，则找到了

  // 删除，判断是否合并或者借节点过来，
  // 删除之后 大于 m/2 正常 ，如果是当前页面的首节点。ok-->更新父节点
  // 删除之后，如果 相邻页面有 > m/2 的部分，则借用
  // 借左面时 更新当前的父节点， 借右面时 更新右面节点的父节点。
  Context ctx;
  (void)ctx;
  if (header_page_id_ == INVALID_PAGE_ID) {
    return;  // 没有 header  表示没有 B+ tree
  }
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  int idx;
  ctx.root_page_id_ = header_page->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {  // 此时 B+ tree  为空
    return;
  }
  auto root_page_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  if (root_page_guard.PageId() == INVALID_PAGE_ID) {
    // 如果 fetch page 失败,
    return;
  }
  page_id_t cur_id = ctx.root_page_id_;
  ctx.write_set_.emplace_back(std::move(root_page_guard));  // 先把root 送进去
  auto cur_page = ctx.write_set_.rbegin()->template As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto inner_page = ctx.write_set_.rbegin()->As<InternalPage>();  // 太 ！ 对 ！ 啦 ！
    // InternalPage* inner_page= reinterpret_cast<InternalPage *>(page); // 虽然这个可以转化， 但是我觉得用整个page
    // 转换不太对，应该用page的date来转换
    if (inner_page->GetSize() > inner_page->GetMinSize() && inner_page->GetSize() > 2) {
      // 祖宗有父亲罩着很安全，不管了
      // 但是父亲不能释放。
      ctx.header_page_->Drop();             // 头节点先放了
      while (ctx.write_set_.size() != 1) {  // 内部节点必定有一个在里面
        ctx.write_set_.begin()->Drop();
        ctx.write_set_.pop_front();
      }
    }
    inner_page->SearchKey(key, comparator_, idx);
    cur_id = inner_page->ValueAt(idx);
    auto cur_page_guard = bpm_->FetchPageWrite(cur_id);

    cur_page = cur_page_guard.As<BPlusTreePage>();
    if (cur_page_guard.PageId() == INVALID_PAGE_ID) {
      // 如果 fetch page 失败,
      return;
    }
    ctx.write_set_.emplace_back(std::move(cur_page_guard));
  }
  auto leaf_page = ctx.write_set_.rbegin()->AsMut<LeafPage>();  // 将这个  data 转换为  leaf_page
  if (!leaf_page->SearchKey(key, comparator_, idx)) {
    return;  // 如果当前的 叶子节点中没有此节点；无目标，返回；
  }
  // 找到了，删除
  leaf_page->DeleteIndex(idx);
  WritePageGuard leaf_page_guard = std::move(*(ctx.write_set_.rbegin()));
  ctx.write_set_.pop_back();  // 把叶子节点取出来
  // 开始安全检查
  // 根节点,不需要满足最小
  if (ctx.IsRootPage(cur_id)) {
    if (leaf_page->GetSize() == 0) {
      // bpm_->DeletePage(cur_id);
      header_page->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }
  page_id_t page_id = INVALID_PAGE_ID;
  bool ok = false;
  int idxx = idx;
  auto fa_page = ctx.write_set_.rbegin()->AsMut<InternalPage>();
  InternalPage *cur_pagee = fa_page;
  fa_page->SearchKey(key, comparator_, idx);
  if (idxx == 0) {
    fa_page->SetKeyAt(idx, leaf_page->KeyAt(0));
  }
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // 需要借用或者合并
    if (idx < fa_page->GetSize() - 1) {  // 当前有右兄弟
      // 找右兄弟借
      page_id = leaf_page->GetNextPageId();
      auto pg = bpm_->FetchPageWrite(page_id);
      auto bro_page = pg.AsMut<LeafPage>();
      if (bro_page->GetSize() > bro_page->GetMinSize()) {
        leaf_page->InsertIndex(leaf_page->GetSize(), bro_page->KeyAt(0), bro_page->ValueAt(0));
        bro_page->DeleteIndex(0);
        fa_page->SetKeyAt(idx + 1, bro_page->KeyAt(0));
        ok = true;
      }
    }
    if (!ok && idx > 0) {  // 当前有左兄弟
      page_id = fa_page->ValueAt(idx - 1);
      auto id = leaf_page_guard.PageId();
      leaf_page_guard.Drop();
      auto pg = bpm_->FetchPageWrite(page_id);
      auto bro_page = pg.AsMut<LeafPage>();
      leaf_page_guard = bpm_->FetchPageWrite(id);
      leaf_page = leaf_page_guard.AsMut<LeafPage>();
      // ctx.write_set_.emplace_back(bpm_->FetchPageWrite(id)); 这里 这个 叶子不需要进去
      if (bro_page->GetSize() > bro_page->GetMinSize()) {
        leaf_page->InsertIndex(0, bro_page->KeyAt(bro_page->GetSize() - 1), bro_page->ValueAt(bro_page->GetSize() - 1));
        bro_page->DeleteIndex(bro_page->GetSize() - 1);
        fa_page->SetKeyAt(idx, leaf_page->KeyAt(0));
        ok = true;
      }
    }
    if (ok) {
      return;
    }
    // 叶子节点借用节点失败了，叶子节点合并
    if (idx < fa_page->GetSize() - 1) {  // 当前有右兄弟
      // 找右兄弟合并
      page_id = leaf_page->GetNextPageId();
      auto pg = bpm_->FetchPageWrite(page_id);
      auto bro_page = pg.AsMut<LeafPage>();
      bro_page->Merge2Other(*leaf_page);
      fa_page->DeleteIndex(idx + 1);
      idxx = idx + 1;
      ok = true;
    }
    if (!ok && idx > 0) {  // 当前有左兄弟
      page_id = fa_page->ValueAt(idx - 1);
      auto id = leaf_page_guard.PageId();
      leaf_page_guard.Drop();
      auto pg = bpm_->FetchPageWrite(page_id);
      auto bro_page = pg.AsMut<LeafPage>();
      leaf_page_guard = bpm_->FetchPageWrite(id);
      leaf_page = leaf_page_guard.AsMut<LeafPage>();
      leaf_page->Merge2Other(*bro_page);
      fa_page->DeleteIndex(idx);
      idxx = idx;
      ok = true;
    }
    cur_pagee = fa_page;
    if (!ok) {
      // 叶子合并失败了，一般不会出现
      return;
    }
    if (ctx.IsRootPage(ctx.write_set_.rbegin()->PageId())) {
      if (fa_page->GetSize() < 2) {
        header_page->root_page_id_ = fa_page->ValueAt(0);
        return;
      }
    }
  }
  leaf_page_guard.Drop();
  // InternalPage *fa_page;
  // 内部节点递归合并

  ctx.write_set_.pop_back();  // 把之前处理过的 fa pop出来,同时drop
  while (!ctx.write_set_.empty()) {
    ok = false;
    if (cur_pagee->GetSize() >= internal_max_size_ / 2 + 1) {
      // if (cur_pagee->GetSize() >= cur_pagee->GetMinSize()) {
      return;  // 递归结束
    }
    fa_page = ctx.write_set_.rbegin()->AsMut<InternalPage>();
    fa_page->SearchKey(key, comparator_, idx);
    if (cur_pagee->GetSize() < internal_max_size_ / 2 + 1 || cur_pagee->GetSize() <= 1) {
      // 需要借用或者合并
      if (idx < fa_page->GetSize() - 1) {  // 当前有右兄弟
        // 找右兄弟借
        page_id = fa_page->ValueAt(idx + 1);
        auto pg = bpm_->FetchPageWrite(page_id);
        auto bro_page = pg.AsMut<InternalPage>();
        if (bro_page->GetSize() > internal_max_size_ / 2 + 1 && bro_page->GetSize() > 2) {
          cur_pagee->InsertIndex(cur_pagee->GetSize(), bro_page->KeyAt(0), bro_page->ValueAt(0));
          bro_page->DeleteIndex(0);
          fa_page->SetKeyAt(idx + 1, bro_page->KeyAt(0));
          ok = true;
        }
      }
      if (!ok && idx > 0) {  // 当前有左兄弟
        page_id = fa_page->ValueAt(idx - 1);
        auto pg = bpm_->FetchPageWrite(page_id);
        auto bro_page = pg.AsMut<InternalPage>();
        if (bro_page->GetSize() > internal_max_size_ / 2 + 1 && bro_page->GetSize() > 2) {
          cur_pagee->InsertIndex(0, bro_page->KeyAt(bro_page->GetSize() - 1),
                                 bro_page->ValueAt(bro_page->GetSize() - 1));
          bro_page->DeleteIndex(bro_page->GetSize() - 1);
          fa_page->SetKeyAt(idx, cur_pagee->KeyAt(0));
          ok = true;
        }
      }
      if (ok) {
        return;
      }

      if (idx < fa_page->GetSize() - 1) {  // 当前有右兄弟
        // 找右兄弟合并
        page_id = fa_page->ValueAt(idx + 1);
        auto pg = bpm_->FetchPageWrite(page_id);
        auto bro_page = pg.AsMut<InternalPage>();
        bro_page->Merge2Other(*cur_pagee);
        fa_page->DeleteIndex(idx + 1);
        ok = true;
      }
      if (!ok && idx > 0) {  // 当前有左兄弟
        page_id = fa_page->ValueAt(idx - 1);
        auto pg = bpm_->FetchPageWrite(page_id);
        auto bro_page = pg.AsMut<InternalPage>();
        cur_pagee->Merge2Other(*bro_page);
        fa_page->DeleteIndex(idx);
        ok = true;
      }
      if (!ok) {
        return;
      }
      cur_pagee = fa_page;
      if (ctx.IsRootPage(ctx.write_set_.rbegin()->PageId())) {
        if (fa_page->GetSize() < 2) {
          header_page->root_page_id_ = fa_page->ValueAt(0);
          return;
        }
      }
      ctx.write_set_.pop_back();  // 把之前处理过的 fa pop出来
    }
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // std::cout << "Begin " << std::endl;
  // 最左面的叶子 构造索引 迭代器
  // 向左 递归  -> 直到 -> is leaf
  if (header_page_id_ == INVALID_PAGE_ID) {
    return End();  // 没有 header  表示没有 B+ tree
  }
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  int idx;
  auto cur_id = header_page->root_page_id_;
  if (cur_id == INVALID_PAGE_ID) {  // 此时 B+ tree  为空
    return End();
  }
  auto cur_page_guard = bpm_->FetchPageRead(cur_id);
  if (cur_page_guard.PageId() == INVALID_PAGE_ID) {
    // 如果 fetch page 失败,
    return End();
  }

  auto cur_page = cur_page_guard.template As<BPlusTreePage>();
  // header_page_guard.Drop();
  while (!cur_page->IsLeafPage()) {
    idx = 0;
    auto *inner_page = cur_page_guard.As<InternalPage>();  // 太 ！ 对 ！ 啦 ！
    // InternalPage* inner_page= reinterpret_cast<InternalPage *>(page); // 虽然这个可以转化， 但是我觉得用整个page
    cur_id = inner_page->ValueAt(idx);
    auto cur_rg = bpm_->FetchPageRead(cur_id);
    cur_page_guard = std::move(cur_rg);
    cur_page = cur_page_guard.As<BPlusTreePage>();
  }
  // auto leaf_page = cur_page_guard.As<LeafPage>();
  return INDEXITERATOR_TYPE(cur_page_guard, 0, comparator_, bpm_, nullptr);
  // INDEXITERATOR_TYPE iterator(leaf_page, idx, comparator_, bpm_);
  // return INDEXITERATOR_TYPE{leaf_page,0,comparator_,bpm_};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::cout << "Begin " << key << std::endl;
  // 从 key 开始的 叶子 迭代器 。
  // GetValue 类似。
  if (header_page_id_ == INVALID_PAGE_ID) {
    return End();  // 没有 header  表示没有 B+ tree
  }
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  int idx;
  auto cur_id = header_page->root_page_id_;
  if (cur_id == INVALID_PAGE_ID) {  // 此时 B+ tree  为空
    return End();
  }
  auto cur_page_guard = bpm_->FetchPageRead(cur_id);
  if (cur_page_guard.PageId() == INVALID_PAGE_ID) {
    // 如果 fetch page 失败,
    return End();
  }
  auto cur_page = cur_page_guard.template As<BPlusTreePage>();
  // header_page_guard.Drop();
  while (!cur_page->IsLeafPage()) {
    auto *inner_page = cur_page_guard.As<InternalPage>();  // 太 ！ 对 ！ 啦 ！
    // InternalPage* inner_page= reinterpret_cast<InternalPage *>(page); // 虽然这个可以转化， 但是我觉得用整个page
    inner_page->SearchKey(key, comparator_, idx);  // 这里的idx 可能是 0但是没有影响
    cur_id = inner_page->ValueAt(idx);
    auto cur_rg = bpm_->FetchPageRead(cur_id);
    cur_page_guard = std::move(cur_rg);
    cur_page = cur_page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = cur_page_guard.As<LeafPage>();
  if (leaf_page->SearchKey(key, comparator_, idx)) {
    return INDEXITERATOR_TYPE(cur_page_guard, idx, comparator_, bpm_, nullptr);
    // INDEXITERATOR_TYPE iterator(leaf_page, idx, comparator_, bpm_);
  }
  // return INDEXITERATOR_TYPE{leaf_page,0,comparator_,bpm_};
  // for (auto iter = Begin(); iter != End(); ++iter) {
  //   KeyType keyy((*iter).first);
  //   if (comparator_(keyy, key) == 0) {
  //     return iter;
  //   }
  // } 始终在 keyy((*iter).first) 中有问题
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // 叶子节点的最后
  // 这种也不太行，被释放，，但是其实可以的，构造函数只是根据这个rg 构造新的 rg 并且拿走了 这个保护
  ReadPageGuard rg;
  return INDEXITERATOR_TYPE{rg, 0, comparator_, nullptr, nullptr};

  // return INDEXITERATOR_TYPE{new ReadPageGuard{nullptr, nullptr}, 0, comparator_, nullptr, nullptr};
  // return INDEXITERATOR_TYPE{(std::make_shared<ReadPageGuard>(nullptr, nullptr)), 0, comparator_, nullptr, nullptr};
  // make_shared 是自己的一种特定的类型，因此不兼容
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  // header-->root ->page_id.
  if (header_page_id_ == INVALID_PAGE_ID) {
    return 0;
  }
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);

  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  auto cur_id = header_page->root_page_id_;
  if (cur_id == INVALID_PAGE_ID) {
    return 0;
  }
  return cur_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
