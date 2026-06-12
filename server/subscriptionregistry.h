#ifndef SUBSCRIPTIONREGISTRY_H
#define SUBSCRIPTIONREGISTRY_H

#include <algorithm>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Owns the bidirectional client <-> topic subscription mapping so the two
// sides can never drift apart. Not thread-safe by design: like the rest of
// the broker state it is owned exclusively by the broker thread.
class SubscriptionRegistry {
public:
  // Returns true if this created a new subscription.
  bool subscribe(const std::string& clientId, const std::string& topic) {
    if (!m_clientTopics[clientId].insert(topic).second) {
      return false;
    }
    m_topicSubscribers[topic].push_back(clientId);
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
    for (const auto& topic : it->second) {
      dropSubscriber(topic, clientId);
    }
    m_clientTopics.erase(it);
  }

  // nullptr when nobody subscribes to the topic.
  const std::vector<std::string>* subscribersOf(const std::string& topic) const {
    auto it = m_topicSubscribers.find(topic);
    return it != m_topicSubscribers.end() ? &it->second : nullptr;
  }

  // nullptr when the client has no subscriptions.
  const std::set<std::string>* subscriptionsOf(const std::string& clientId) const {
    auto it = m_clientTopics.find(clientId);
    return it != m_clientTopics.end() ? &it->second : nullptr;
  }

private:
  void dropSubscriber(const std::string& topic, const std::string& clientId) {
    auto it = m_topicSubscribers.find(topic);
    if (it == m_topicSubscribers.end()) {
      return;
    }

    auto& subs = it->second;
    auto pos = std::find(subs.begin(), subs.end(), clientId);
    if (pos != subs.end()) {
      *pos = subs.back();
      subs.pop_back();
    }

    if (subs.empty()) {
      m_topicSubscribers.erase(it);
    }
  }

  std::unordered_map<std::string, std::set<std::string>> m_clientTopics;
  std::unordered_map<std::string, std::vector<std::string>> m_topicSubscribers;
};

#endif  // SUBSCRIPTIONREGISTRY_H
