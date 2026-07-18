#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <thread>

#include "safequeue.h"

using namespace std::chrono_literals;

TEST(SafeQueueTest, PushThenPopPreservesFifoOrder) {
  SafeQueue<int> queue;
  ASSERT_TRUE(queue.push(1));
  ASSERT_TRUE(queue.push(2));
  ASSERT_TRUE(queue.push(3));

  int value = 0;
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 1);
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 2);
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 3);
}

TEST(SafeQueueTest, TryPopOnEmptyQueueReturnsFalseImmediately) {
  SafeQueue<int> queue;
  int value = 0;

  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(queue.try_pop(value));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 50ms) << "try_pop() should return immediately rather than waiting";
}

TEST(SafeQueueTest, PopBlocksUntilValuePushed) {
  SafeQueue<int> queue;
  std::atomic<bool> popped{false};
  int result = 0;

  std::thread popper([&]() {
    queue.pop(result);
    popped = true;
  });

  // The queue starts empty, so pop() cannot succeed before the push() below
  // runs - this isn't a timing guess, it's the blocking contract under test.
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(popped) << "pop() returned before any value was pushed";

  ASSERT_TRUE(queue.push(42));

  popper.join();
  EXPECT_TRUE(popped);
  EXPECT_EQ(result, 42);
}

TEST(SafeQueueTest, PushBlocksWhenQueueIsFull) {
  SafeQueue<int> queue(1);
  ASSERT_TRUE(queue.push(1));

  std::atomic<bool> pushed{false};
  std::thread pusher([&]() {
    queue.push(2);
    pushed = true;
  });

  // With maxSize == 1 and one item already enqueued, push(2) cannot complete
  // until the pop() below frees a slot - guaranteed by the capacity contract.
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(pushed) << "push() returned before the queue had room";

  int value = 0;
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 1);

  pusher.join();
  EXPECT_TRUE(pushed);

  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 2);
}

TEST(SafeQueueTest, PushWithTimeoutFailsWhenQueueStaysFull) {
  SafeQueue<int> queue(1);
  ASSERT_TRUE(queue.push(1));

  EXPECT_FALSE(queue.push(2, 50ms));

  int value = 0;
  ASSERT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 1) << "push() with timeout should not have enqueued anything once it gave up";
  EXPECT_FALSE(queue.try_pop(value));
}

TEST(SafeQueueTest, PushWithTimeoutSucceedsWhenSpaceFreesBeforeDeadline) {
  SafeQueue<int> queue(1);
  ASSERT_TRUE(queue.push(1));

  std::thread freer([&]() {
    std::this_thread::sleep_for(30ms);
    int discarded = 0;
    queue.pop(discarded);
  });

  EXPECT_TRUE(queue.push(2, 1000ms));
  freer.join();

  int value = 0;
  ASSERT_TRUE(queue.try_pop(value));
  EXPECT_EQ(value, 2);
}

TEST(SafeQueueTest, StopUnblocksWaitingPopAndReturnsFalse) {
  SafeQueue<int> queue;
  std::atomic<bool> finished{false};
  bool result = true;
  int value = 0;

  std::thread popper([&]() {
    result = queue.pop(value);
    finished = true;
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(finished);

  queue.stop();
  popper.join();

  EXPECT_TRUE(finished);
  EXPECT_FALSE(result) << "pop() should report failure once the queue is stopped";
}

TEST(SafeQueueTest, StopUnblocksWaitingPushAndReturnsFalse) {
  SafeQueue<int> queue(1);
  ASSERT_TRUE(queue.push(1));

  std::atomic<bool> finished{false};
  bool result = true;

  std::thread pusher([&]() {
    result = queue.push(2);
    finished = true;
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(finished);

  queue.stop();
  pusher.join();

  EXPECT_TRUE(finished);
  EXPECT_FALSE(result) << "push() should report failure once the queue is stopped";
}

TEST(SafeQueueTest, StopAllowsDrainingRemainingItemsBeforePopReturnsFalse) {
  SafeQueue<int> queue;
  ASSERT_TRUE(queue.push(1));
  ASSERT_TRUE(queue.push(2));

  queue.stop();

  int value = 0;
  ASSERT_TRUE(queue.pop(value)) << "pop() should still drain items that were enqueued before stop()";
  EXPECT_EQ(value, 1);
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 2);

  EXPECT_FALSE(queue.pop(value)) << "pop() should fail once the queue is both empty and stopped";
}

TEST(SafeQueueTest, PushAfterStopReturnsFalseImmediately) {
  SafeQueue<int> queue;
  queue.stop();

  EXPECT_FALSE(queue.push(1));

  int value = 0;
  EXPECT_FALSE(queue.try_pop(value)) << "push() after stop() should not have enqueued anything";
}

TEST(SafeQueueTest, DrainToHandsOverEverythingInOrder) {
  SafeQueue<int> queue;
  ASSERT_TRUE(queue.push(1));
  ASSERT_TRUE(queue.push(2));
  ASSERT_TRUE(queue.push(3));

  std::deque<int> batch{99};  // pre-existing content must be discarded
  EXPECT_EQ(queue.drainTo(batch), 3u);
  ASSERT_EQ(batch.size(), 3u);
  EXPECT_EQ(batch[0], 1);
  EXPECT_EQ(batch[1], 2);
  EXPECT_EQ(batch[2], 3);

  int value = 0;
  EXPECT_FALSE(queue.try_pop(value)) << "the queue should be empty after a drain";
  EXPECT_EQ(queue.drainTo(batch), 0u);
  EXPECT_TRUE(batch.empty());
}

TEST(SafeQueueTest, DrainToUnblocksAWaitingPush) {
  SafeQueue<int> queue(2);
  ASSERT_TRUE(queue.push(1));
  ASSERT_TRUE(queue.push(2));

  std::atomic<bool> pushed{false};
  std::thread pusher([&]() {
    queue.push(3);
    pushed = true;
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(pushed) << "push() returned before the queue had room";

  std::deque<int> batch;
  EXPECT_EQ(queue.drainTo(batch), 2u);

  pusher.join();
  EXPECT_TRUE(pushed);
}

TEST(SafeQueueTest, PushReportsWhetherTheQueueWasEmpty) {
  SafeQueue<int> queue;

  bool wasEmpty = false;
  ASSERT_TRUE(queue.push(1, wasEmpty));
  EXPECT_TRUE(wasEmpty) << "the first push lands in an empty queue";

  ASSERT_TRUE(queue.push(2, wasEmpty));
  EXPECT_FALSE(wasEmpty) << "the queue already held an item";

  std::deque<int> batch;
  queue.drainTo(batch);

  ASSERT_TRUE(queue.push(3, 50ms, wasEmpty));
  EXPECT_TRUE(wasEmpty) << "a drain empties the queue again";
}
