/**
 * deadlock_detection_test.cpp
 */

#include <atomic>
#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {
TEST(LockManagerDeadlockDetectionTest, EdgeTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.push_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(txn_ids.begin(), txn_ids.end(), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

TEST(LockManagerDeadlockDetectionTest, MyGraphTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  lock_mgr.AddEdge(76, 39);
  lock_mgr.AddEdge(21, 44);
  lock_mgr.AddEdge(25, 82);
  lock_mgr.AddEdge(34, 72);
  lock_mgr.AddEdge(94, 1);
  lock_mgr.AddEdge(6, 53);
  lock_mgr.AddEdge(89, 48);
  lock_mgr.AddEdge(4, 74);
  lock_mgr.AddEdge(57, 60);
  lock_mgr.AddEdge(23, 52);
  lock_mgr.AddEdge(50, 27);
  lock_mgr.AddEdge(46, 80);
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 0);
  txn_id_t victim;
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(victim, 1);
  lock_mgr.AddEdge(2, 3);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(victim, 1);
  lock_mgr.AddEdge(3, 4);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(victim, 1);
  lock_mgr.AddEdge(4, 2);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(1, victim);
  lock_mgr.RemoveEdge(1, 0);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(4, victim);
  lock_mgr.RemoveEdge(4, 2);
  EXPECT_EQ(false, lock_mgr.HasCycle(&victim));
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 2);
  lock_mgr.AddEdge(2, 3);
  lock_mgr.AddEdge(3, 4);
  lock_mgr.AddEdge(4, 5);
  lock_mgr.AddEdge(5, 0);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(5, victim);
  lock_mgr.AddEdge(2, 6);
  lock_mgr.AddEdge(6, 7);
  lock_mgr.AddEdge(7, 2);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(5, victim);
  lock_mgr.RemoveEdge(5, 0);
  EXPECT_EQ(true, lock_mgr.HasCycle(&victim));
  EXPECT_EQ(7, victim);
  lock_mgr.RemoveEdge(7, 2);
  EXPECT_EQ(false, lock_mgr.HasCycle(&victim));
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}

TEST(LockManagerDeadlockDetectionTest, MyComplexDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  RID rid2{2, 2};

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn1, toid, rid2);
    lock_mgr.UnlockRow(txn1, toid, rid1);
    lock_mgr.UnlockTable(txn1, toid);

    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    txn_mgr.Abort(txn2);
  });

  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerDeadlockDetectionTest, MyMoreComplexDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  RID rid2{2, 2};

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());

  std::thread t0([&] {
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(res, true);

    lock_mgr.UnlockRow(txn1, toid, rid2);
    lock_mgr.UnlockRow(txn1, toid, rid1);
    lock_mgr.UnlockTable(txn1, toid);

    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid2);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.LockRow(txn2, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    txn_mgr.Abort(txn2);
  });

  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerDeadlockDetectionTest, MySuperComplexDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid0{0};
  table_oid_t toid1{1};
  table_oid_t toid2{2};
  table_oid_t toid3{3};
  table_oid_t toid4{4};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  auto *txn3 = txn_mgr.Begin();
  auto *txn4 = txn_mgr.Begin();
  auto *txn5 = txn_mgr.Begin();
  auto *txn6 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());
  EXPECT_EQ(2, txn2->GetTransactionId());
  EXPECT_EQ(3, txn3->GetTransactionId());
  EXPECT_EQ(4, txn4->GetTransactionId());
  EXPECT_EQ(5, txn5->GetTransactionId());
  EXPECT_EQ(6, txn6->GetTransactionId());

  std::thread t0([&] {
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, toid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    lock_mgr.UnlockTable(txn0, toid0);
    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, toid1);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // block here
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, toid2);
    EXPECT_EQ(res, true);
    lock_mgr.UnlockTable(txn1, toid1);
    lock_mgr.UnlockTable(txn1, toid2);
    txn_mgr.Commit(txn1);
    EXPECT_EQ(TransactionState::COMMITTED, txn1->GetState());
  });

  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, toid3);
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, toid2);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn2->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // block here
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, toid4);
    EXPECT_EQ(true, res);
    lock_mgr.UnlockTable(txn2, toid3);
    lock_mgr.UnlockTable(txn2, toid4);
    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });

  std::thread t3([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    bool res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_SHARED, toid0);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockTable(txn3, LockManager::LockMode::INTENTION_SHARED, toid4);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn3->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // block here
    res = lock_mgr.LockTable(txn3, LockManager::LockMode::EXCLUSIVE, toid1);
    EXPECT_EQ(res, false);
    EXPECT_EQ(TransactionState::ABORTED, txn3->GetState());
    txn_mgr.Abort(txn3);
  });

  std::thread t4([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    bool res = lock_mgr.LockTable(txn4, LockManager::LockMode::INTENTION_SHARED, toid4);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn4->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // block here
    res = lock_mgr.LockTable(txn4, LockManager::LockMode::EXCLUSIVE, toid0);
    EXPECT_EQ(res, true);
    lock_mgr.UnlockTable(txn4, toid4);
    lock_mgr.UnlockTable(txn4, toid0);
    txn_mgr.Commit(txn4);
    EXPECT_EQ(TransactionState::COMMITTED, txn4->GetState());
  });

  std::thread t5([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // block here
    bool res = lock_mgr.LockTable(txn5, LockManager::LockMode::EXCLUSIVE, toid3);
    EXPECT_EQ(res, true);
    lock_mgr.UnlockTable(txn5, toid3);
    txn_mgr.Commit(txn5);
    EXPECT_EQ(TransactionState::COMMITTED, txn5->GetState());
  });

  std::thread t6([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn6, LockManager::LockMode::INTENTION_SHARED, toid2);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn6->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    lock_mgr.UnlockTable(txn6, toid2);
    txn_mgr.Commit(txn6);
    EXPECT_EQ(TransactionState::COMMITTED, txn6->GetState());
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 5);

  t0.join();
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  t6.join();

  delete txn0;
  delete txn1;
  delete txn2;
  delete txn3;
  delete txn4;
  delete txn5;
  delete txn6;
}
}  // namespace bustub
