#ifndef WORKERINTERFACE_H
#define WORKERINTERFACE_H

#include <functional>
#include <string>

#include "wireframe.h"

using WorkerMessageCallback = std::function<void(const Envelope&)>;
using WorkerStatusCallback = std::function<void(bool)>;

class WorkerInterface {
public:
  virtual ~WorkerInterface() = default;

  virtual void start() = 0;
  virtual void stop() = 0;

  // Sink parameters: pass with std::move to hand the envelope over without
  // copying its payload; passing an lvalue costs one copy.
  virtual bool writeMessage(Envelope msg) = 0;
  virtual bool writeControlMessage(Envelope msg) = 0;

  virtual void setMessageCallback(WorkerMessageCallback cb) = 0;
};

#endif  // WORKERINTERFACE_H
