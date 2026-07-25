#ifndef WISP_CLI_PAYLOADFORMAT_H
#define WISP_CLI_PAYLOADFORMAT_H

#include <string>

#include "options.h"

namespace WispCli {

/* Render a payload frame as one line of display text.

   Auto tries, in order: a packed protobuf Any whose type this binary knows (the
   broker's own messages), text when every byte is printable, hex otherwise.
   Text and Hex force those two; both escape or drop nothing else. Rendering
   stops after `maxBytes` bytes and says how many were left.

   PayloadFormat::Raw is not handled here - a caller wanting the bytes verbatim
   writes `payload` straight to stdout. */
std::string renderPayload(const std::string& payload, PayloadFormat format, int maxBytes);

// True when `payload` holds nothing but printable characters, tabs and
// newlines included. High bytes pass so UTF-8 text is not mistaken for binary.
bool looksLikeText(const std::string& payload);

// Spaced lowercase hex ("0a 1f 3c"), truncated to maxBytes.
std::string toHex(const std::string& bytes, int maxBytes);

/* One-line protobuf text format for a payload holding a packed Any, e.g.
   `broker.SystemStats { broker_id: "..." clients_count: 2 }`.

   Empty when the payload is not an Any, or when its type was not compiled into
   this binary - the same limit the inspector has, since both resolve types
   through protobuf's generated descriptor pool. */
std::string renderPackedAny(const std::string& payload);

}  // namespace WispCli

#endif  // WISP_CLI_PAYLOADFORMAT_H
