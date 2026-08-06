#ifndef SUBSCRIPTIONREGISTRY_H
#define SUBSCRIPTIONREGISTRY_H

#include <algorithm>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "messagekeys.h"

namespace Wisp {

// A client's subscription to one topic, and the origins it wants delivered on
// it (see Origin).
struct Subscriber {
  std::string clientId;
  Origin scope;
};

// Owns the bidirectional client <-> topic subscription mapping so the two
// sides can never drift apart. Not thread-safe by design: like the rest of
// the broker state it is owned exclusively by the broker thread.
class SubscriptionRegistry {
public:
  /* Returns true if this created a subscription or changed an existing one's
     scope - the cases in which what the broker should carry may have moved.
     Re-subscribing with the same scope is the idempotent no-op a RESET
     recovery relies on. */
  bool subscribe(const std::string& clientId, const std::string& topic, Origin scope = Origin::Any) {
    auto [held, inserted] = m_clientTopics[clientId].emplace(topic, scope);
    if (inserted) {
      m_topicSubscribers[topic].push_back({clientId, scope});
      return true;
    }
    if (held->second == scope) {
      return false;
    }
    held->second = scope;
    if (Subscriber* subscriber = findSubscriber(topic, clientId)) {
      subscriber->scope = scope;
    }
    return true;
  }

  // Returns true if the subscription existed.
  bool unsubscribe(const std::string& clientId, const std::string& topic) {
    auto it = m_clientTopics.find(clientId);
    if (it == m_clientTopics.end() || it->second.erase(topic) == 0) {
      return false;
    }
    if (it->second.empty()) {
      m_clientTopics.erase(it);
    }
    dropSubscriber(topic, clientId);
    return true;
  }

  void removeClient(const std::string& clientId) {
    auto it = m_clientTopics.find(clientId);
    if (it == m_clientTopics.end()) {
      return;
    }
    for (const auto& entry : it->second) {
      dropSubscriber(entry.first, clientId);
    }
    m_clientTopics.erase(it);
  }

  // nullptr when nobody subscribes to the topic.
  const std::vector<Subscriber>* subscribersOf(const std::string& topic) const {
    auto it = m_topicSubscribers.find(topic);
    return it != m_topicSubscribers.end() ? &it->second : nullptr;
  }

  // nullptr when the client has no subscriptions.
  const std::map<std::string, Origin>* subscriptionsOf(const std::string& clientId) const {
    auto it = m_clientTopics.find(clientId);
    return it != m_clientTopics.end() ? &it->second : nullptr;
  }

  /* Whether anyone here wants this topic from across a peer link - which is
     what makes the broker carry interest in it into the mesh. Scanned rather
     than counted: it is asked only as a subscription appears or disappears,
     never per message, and a count is one more thing to keep in step with the
     two maps. */
  bool hasMeshSubscriber(const std::string& topic) const {
    const std::vector<Subscriber>* subscribers = subscribersOf(topic);
    if (!subscribers) {
      return false;
    }
    return std::any_of(subscribers->begin(), subscribers->end(), [](const Subscriber& subscriber) { return scopeAccepts(subscriber.scope, false); });
  }

private:
  Subscriber* findSubscriber(const std::string& topic, const std::string& clientId) {
    auto it = m_topicSubscribers.find(topic);
    if (it == m_topicSubscribers.end()) {
      return nullptr;
    }
    auto pos = std::find_if(it->second.begin(), it->second.end(), [&](const Subscriber& subscriber) { return subscriber.clientId == clientId; });
    return pos != it->second.end() ? &*pos : nullptr;
  }

  void dropSubscriber(const std::string& topic, const std::string& clientId) {
    auto it = m_topicSubscribers.find(topic);
    if (it == m_topicSubscribers.end()) {
      return;
    }

    auto& subs = it->second;
    auto pos = std::find_if(subs.begin(), subs.end(), [&](const Subscriber& subscriber) { return subscriber.clientId == clientId; });
    if (pos != subs.end()) {
      *pos = subs.back();
      subs.pop_back();
    }

    if (subs.empty()) {
      m_topicSubscribers.erase(it);
    }
  }

  std::unordered_map<std::string, std::map<std::string, Origin>> m_clientTopics;
  std::unordered_map<std::string, std::vector<Subscriber>> m_topicSubscribers;
};

}  // namespace Wisp

#endif  // SUBSCRIPTIONREGISTRY_H
