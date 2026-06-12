#include <gtest/gtest.h>

#include "subscriptionregistry.h"

TEST(SubscriptionRegistryTest, SubscribeIsIdempotent) {
  SubscriptionRegistry registry;

  EXPECT_TRUE(registry.subscribe("client-a", "topic-1"));
  EXPECT_FALSE(registry.subscribe("client-a", "topic-1"));

  const auto* subs = registry.subscribersOf("topic-1");
  ASSERT_NE(subs, nullptr);
  EXPECT_EQ(subs->size(), 1u);
}

TEST(SubscriptionRegistryTest, UnsubscribeRemovesBothDirections) {
  SubscriptionRegistry registry;
  registry.subscribe("client-a", "topic-1");

  EXPECT_TRUE(registry.unsubscribe("client-a", "topic-1"));
  EXPECT_FALSE(registry.unsubscribe("client-a", "topic-1"));

  EXPECT_EQ(registry.subscribersOf("topic-1"), nullptr);
  EXPECT_EQ(registry.subscriptionsOf("client-a"), nullptr);
}

TEST(SubscriptionRegistryTest, UnsubscribeForUnknownClientIsHarmless) {
  SubscriptionRegistry registry;

  EXPECT_FALSE(registry.unsubscribe("ghost", "topic-1"));
  EXPECT_EQ(registry.subscribersOf("topic-1"), nullptr);
}

TEST(SubscriptionRegistryTest, RemoveClientDropsAllItsSubscriptionsAndOnlyThose) {
  SubscriptionRegistry registry;
  registry.subscribe("client-a", "topic-1");
  registry.subscribe("client-a", "topic-2");
  registry.subscribe("client-b", "topic-1");

  registry.removeClient("client-a");

  EXPECT_EQ(registry.subscriptionsOf("client-a"), nullptr);
  EXPECT_EQ(registry.subscribersOf("topic-2"), nullptr);

  const auto* topic1Subs = registry.subscribersOf("topic-1");
  ASSERT_NE(topic1Subs, nullptr);
  ASSERT_EQ(topic1Subs->size(), 1u);
  EXPECT_EQ((*topic1Subs)[0], "client-b");
}

TEST(SubscriptionRegistryTest, EmptyTopicEntriesAreErasedNotLeftEmpty) {
  SubscriptionRegistry registry;
  registry.subscribe("client-a", "topic-1");
  registry.subscribe("client-b", "topic-1");

  registry.removeClient("client-a");
  EXPECT_TRUE(registry.unsubscribe("client-b", "topic-1"));

  // The delivery path treats nullptr as "no subscribers"; a stale empty
  // vector would make it build and copy messages for nobody.
  EXPECT_EQ(registry.subscribersOf("topic-1"), nullptr);
}

TEST(SubscriptionRegistryTest, EmptyTopicIsAnOrdinaryKey) {
  SubscriptionRegistry registry;

  // The wildcard semantics of "" live in the broker; the registry must just
  // store it like any other topic.
  EXPECT_TRUE(registry.subscribe("peer-link", ""));
  const auto* subs = registry.subscribersOf("");
  ASSERT_NE(subs, nullptr);
  EXPECT_EQ((*subs)[0], "peer-link");
}
