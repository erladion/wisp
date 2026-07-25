#ifndef DATAMODEL_H
#define DATAMODEL_H

#include <cstdint>
#include <string>

#include "broker.pb.h"


struct InspectorPacket {
  std::string timestamp;
  std::string senderId;
  std::string key;
  std::string topic;
  size_t sizeBytes;  // header + payload bytes on the wire

  broker::MessageHeader header;
  std::string payload;  // opaque payload frame bytes
};

Q_DECLARE_METATYPE(InspectorPacket)

#endif