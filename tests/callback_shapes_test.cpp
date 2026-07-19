#include <gtest/gtest.h>

#include <functional>
#include <string>

#include "connectionmanager.h"

/* registerCallback() accepts several callable shapes, and picking the wrong
   overload is a *compile* error rather than a test failure - so what these
   cases mostly assert is that they build at all.

   Three shapes below did not, before this file existed:

     - a `mutable` no-argument lambda, whose operator() is non-const: with only
       the `R (C::*)() const` specialization of CallableTraits it fell into the
       primary template, which asked a member-pointer type for its own
       operator() and hard-errored;
     - a plain no-argument function pointer, which failed the same way for want
       of an `R (*)()` specialization;
     - a `mutable` lambda taking an argument, which could not be invoked
       through the const wrapper lambda that dispatch built around it.

   Captureless no-argument lambdas were separately ambiguous while both a
   void(*)() and a std::function<void()> overload existed - a std::function
   accepts function pointers, so only the latter remains.

   Every failure was raised deep inside template instantiation, nowhere near
   the call site. Cheap to keep working, expensive to diagnose once broken.

   No broker is involved: registrations made before ConnectionManager::init()
   are parked in the pending list, which is exactly the path exercised here. */

namespace {
const std::string kTopic = "callback-shapes-topic";

void freeFunctionWithArg(const std::string&) {}
void freeFunctionNoArgs() {}

struct Receiver {
  void withArg(const std::string&) { ++calls; }
  void noArgs() { ++calls; }
  int calls = 0;
};
}  // namespace

TEST(CallbackShapesTest, EveryCallableShapeRegisters) {
  int owner = 0;
  Receiver receiver;

  // Typed lambdas: dispatch on the argument type via CallableTraits.
  ConnectionManager::registerCallback(kTopic, [](const std::string&) {}, &owner);
  ConnectionManager::registerCallback(kTopic, [](int) {}, &owner);

  // No-argument lambdas, both cv-forms of operator(). The mutable one is the
  // regression case: it must not be a hard compile error.
  ConnectionManager::registerCallback(kTopic, []() {}, &owner);
  ConnectionManager::registerCallback(kTopic, []() mutable {}, &owner);

  // A mutable lambda that also takes an argument - non-const operator() on the
  // typed path, which is the other half of the same gap.
  int captured = 0;
  ConnectionManager::registerCallback(kTopic, [captured](const std::string&) mutable { ++captured; }, &owner);

  ConnectionManager::registerCallback(kTopic, &freeFunctionWithArg, &owner);
  ConnectionManager::registerCallback(kTopic, &freeFunctionNoArgs, &owner);

  ConnectionManager::registerCallback(kTopic, &Receiver::withArg, &receiver);
  ConnectionManager::registerCallback(kTopic, &Receiver::noArgs, &receiver);

  ConnectionManager::registerCallback(kTopic, std::function<void()>([] {}), &owner);

  // Reaching here means every shape above resolved to an overload.
  SUCCEED();

  ConnectionManager::unregisterCallback(kTopic, &owner);
  ConnectionManager::unregisterCallback(kTopic, &receiver);
}
