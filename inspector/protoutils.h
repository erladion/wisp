#ifndef PROTOUTILS_H
#define PROTOUTILS_H

#include <google/protobuf/any.pb.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <QTreeWidget>
#include <QTreeWidgetItem>

#include <algorithm>
#include <memory>
#include <string>

#include "broker.pb.h"
#include "connectionmanager.h"  // detail::kAnyTypeUrlPrefix - the same prefix the client library writes

namespace ProtoUtils {

// Bytes fields (e.g. the 16-byte binary message uuid) render as spaced hex -
// TextFormat would escape them into unreadable \x noise.
inline QString formatBytesValue(const std::string& bytes) {
  static const char hexChars[] = "0123456789abcdef";
  const std::size_t shown = std::min<std::size_t>(bytes.size(), 64);

  QString out;
  out.reserve(static_cast<int>(shown) * 3 + 16);
  for (std::size_t i = 0; i < shown; ++i) {
    const auto c = static_cast<unsigned char>(bytes[i]);
    if (i > 0) {
      out += ' ';
    }
    out += hexChars[c >> 4];
    out += hexChars[c & 0xf];
  }
  if (bytes.size() > shown) {
    out += QString(" … (+%1 bytes)").arg(bytes.size() - shown);
  }
  return out;
}

inline void populateProtobufTree(const google::protobuf::Message& msg, QTreeWidgetItem* parentNode) {
  const google::protobuf::Descriptor* descriptor = msg.GetDescriptor();
  const google::protobuf::Reflection* reflection = msg.GetReflection();

  for (int i = 0; i < descriptor->field_count(); ++i) {
    const google::protobuf::FieldDescriptor* field = descriptor->field(i);

    if (!field->is_repeated() && !reflection->HasField(msg, field)) {
      continue;
    }

    QTreeWidgetItem* fieldNode = new QTreeWidgetItem(parentNode);
    fieldNode->setText(0, QString::fromStdString(std::string(field->name())));

    // Handle nested messages (and arrays of nested messages)
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      if (field->is_repeated()) {
        int count = reflection->FieldSize(msg, field);
        fieldNode->setText(1, QString("[%1 items]").arg(count));
        for (int j = 0; j < count; ++j) {
          QTreeWidgetItem* itemNode = new QTreeWidgetItem(fieldNode);
          itemNode->setText(0, QString("[%1]").arg(j));
          populateProtobufTree(reflection->GetRepeatedMessage(msg, field, j), itemNode);
        }
      } else {
        fieldNode->setText(1, "{...}");
        populateProtobufTree(reflection->GetMessage(msg, field), fieldNode);
      }
    } else {                       // Handle standard types (Ints, Strings, Bools, etc)
      if (field->is_repeated()) {  // Handle arrays of standard types (like repeated string)
        int count = reflection->FieldSize(msg, field);
        fieldNode->setText(1, QString("[%1 items]").arg(count));
        for (int j = 0; j < count; ++j) {
          QTreeWidgetItem* itemNode = new QTreeWidgetItem(fieldNode);
          itemNode->setText(0, QString("[%1]").arg(j));

          if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
            itemNode->setText(1, formatBytesValue(reflection->GetRepeatedString(msg, field, j)));
            continue;
          }
          std::string valueStr;
          google::protobuf::TextFormat::PrintFieldValueToString(msg, field, j, &valueStr);
          if (!valueStr.empty() && valueStr.back() == '\n') {
            valueStr.pop_back();
          }
          itemNode->setText(1, QString::fromStdString(valueStr));
        }
      } else if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
        fieldNode->setText(1, formatBytesValue(reflection->GetString(msg, field)));
      } else {
        std::string valueStr;
        google::protobuf::TextFormat::PrintFieldValueToString(msg, field, -1, &valueStr);

        if (!valueStr.empty() && valueStr.back() == '\n') {
          valueStr.pop_back();
        }
        fieldNode->setText(1, QString::fromStdString(valueStr));
      }
    }
  }
}

inline std::unique_ptr<google::protobuf::Message> dynamicallyUnpack(const google::protobuf::Any& any) {
  std::string typeUrl = any.type_url();
  std::string typeName = typeUrl.substr(typeUrl.find_last_of('/') + 1);

  const google::protobuf::Descriptor* descriptor = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(typeName);
  if (!descriptor) {
    return nullptr;
  }

  const google::protobuf::Message* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
  if (!prototype) {
    return nullptr;
  }

  std::unique_ptr<google::protobuf::Message> dynamicMsg(prototype->New());
  if (any.UnpackTo(dynamicMsg.get())) {
    return dynamicMsg;
  }
  return nullptr;
}

inline void drawEnvelopeAndPayload(const broker::MessageHeader& header, const std::string& payload, QTreeWidget* treeWidget) {
  QTreeWidgetItem* envelopeRoot = new QTreeWidgetItem(treeWidget);
  envelopeRoot->setText(0, "Message Header");
  populateProtobufTree(header, envelopeRoot);
  envelopeRoot->setExpanded(true);

  if (payload.empty()) {
    return;
  }

  // The payload frame is opaque. Protobuf payloads (from the client lib and the
  // broker's stats) arrive as a packed Any; try that first and fall back to a
  // raw byte view for anything else (JSON, structs, plain strings).
  google::protobuf::Any any;
  if (any.ParseFromString(payload) && any.type_url().rfind(std::string(detail::kAnyTypeUrlPrefix), 0) == 0) {
    auto dynamicPayload = dynamicallyUnpack(any);
    if (dynamicPayload) {
      QTreeWidgetItem* payloadRoot = new QTreeWidgetItem(treeWidget);
      payloadRoot->setText(0, QString::fromStdString(std::string(dynamicPayload->GetTypeName())));
      populateProtobufTree(*dynamicPayload, payloadRoot);
      payloadRoot->setExpanded(true);
    } else {
      QTreeWidgetItem* errorRoot = new QTreeWidgetItem(treeWidget);
      errorRoot->setText(0, "[Unknown Protobuf Schema]");
      errorRoot->setText(1, "Link .pb.cc file to view");
    }
    return;
  }

  QTreeWidgetItem* rawRoot = new QTreeWidgetItem(treeWidget);
  rawRoot->setText(0, "Raw Payload");
  rawRoot->setText(1, QString("[%1 bytes]").arg(payload.size()));
}

}  // namespace ProtoUtils

#endif
