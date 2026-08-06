#include <gtest/gtest.h>

#include "subscriptionregistry.h"

using namespace Wisp;

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
  EXPECT_EQ((*topic1Subs)[0].clientId, "client-b");
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

/* The broker decides whether to carry a topic into the mesh by comparing this
   answer before and after touching the registry, so a scope change has to
   report as a change - a re-subscribe that narrows one is otherwise
   indistinguishable from the idempotent one a RESET recovery sends. */
TEST(SubscriptionRegistryTest, ChangingAScopeCountsAsAChangeButRepeatingOneDoesNot) {
  SubscriptionRegistry registry;

  EXPECT_TRUE(registry.subscribe("client-a", "topic-1", Origin::Local));
  EXPECT_FALSE(registry.subscribe("client-a", "topic-1", Origin::Local));
  EXPECT_TRUE(registry.subscribe("client-a", "topic-1", Origin::Any));

  const auto* subs = registry.subscribersOf("topic-1");
  ASSERT_NE(subs, nullptr);
  ASSERT_EQ(subs->size(), 1u) << "a scope change must update the subscription, not add a second one";
  EXPECT_EQ((*subs)[0].scope, Origin::Any);
}

TEST(SubscriptionRegistryTest, MeshInterestIsHeldByAnySubscriberThatWantsIt) {
  SubscriptionRegistry registry;

  registry.subscribe("local-only", "topic-1", Origin::Local);
  EXPECT_FALSE(registry.hasMeshSubscriber("topic-1"));

  registry.subscribe("wants-mesh", "topic-1", Origin::Any);
  EXPECT_TRUE(registry.hasMeshSubscriber("topic-1"));

  // The local-only one leaving changes nothing; it never held the interest.
  registry.unsubscribe("local-only", "topic-1");
  EXPECT_TRUE(registry.hasMeshSubscriber("topic-1"));

  registry.unsubscribe("wants-mesh", "topic-1");
  EXPECT_FALSE(registry.hasMeshSubscriber("topic-1"));
}

// Narrowing a scope withdraws mesh interest exactly as unsubscribing does -
// the same client, still subscribed, just no longer wanting what a link
// carries.
TEST(SubscriptionRegistryTest, NarrowingTheLastMeshSubscriberDropsMeshInterest) {
  SubscriptionRegistry registry;
  registry.subscribe("client-a", "topic-1", Origin::Any);
  ASSERT_TRUE(registry.hasMeshSubscriber("topic-1"));

  EXPECT_TRUE(registry.subscribe("client-a", "topic-1", Origin::Local));
  EXPECT_FALSE(registry.hasMeshSubscriber("topic-1"));
}

TEST(SubscriptionRegistryTest, RemoveClientTakesItsMeshInterestWithIt) {
  SubscriptionRegistry registry;
  registry.subscribe("client-a", "topic-1", Origin::Mesh);
  registry.subscribe("client-b", "topic-1", Origin::Local);
  ASSERT_TRUE(registry.hasMeshSubscriber("topic-1"));

  registry.removeClient("client-a");
  EXPECT_FALSE(registry.hasMeshSubscriber("topic-1")) << "the only subscriber wanting the mesh is gone";
  EXPECT_NE(registry.subscribersOf("topic-1"), nullptr) << "the local-only subscriber must survive";
}

TEST(SubscriptionRegistryTest, ScopelessSubscribersWantEverything) {
  SubscriptionRegistry registry;

  // What every client sent before scopes existed, and what an unset scope off
  // the wire decodes to.
  registry.subscribe("client-a", "topic-1");

  const auto* subs = registry.subscribersOf("topic-1");
  ASSERT_NE(subs, nullptr);
  EXPECT_EQ((*subs)[0].scope, Origin::Any);
  EXPECT_TRUE(registry.hasMeshSubscriber("topic-1"));
}

TEST(SubscriptionRegistryTest, EmptyTopicIsAnOrdinaryKey) {
  SubscriptionRegistry registry;

  // The wildcard semantics of "" live in the broker; the registry must just
  // store it like any other topic.
  EXPECT_TRUE(registry.subscribe("peer-link", ""));
  const auto* subs = registry.subscribersOf("");
  ASSERT_NE(subs, nullptr);
  EXPECT_EQ((*subs)[0].clientId, "peer-link");
}
