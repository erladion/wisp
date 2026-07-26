#include "payloadformat.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <string_view>

#include "anyframe.h"  // reading the Any framing a payload carries, without knowing its type

namespace WispCli {

namespace {

constexpr std::size_t DECODED_WIDTH_FACTOR = 4;

// Printable ASCII, the three whitespace characters worth passing through, and
// anything above 0x7f (UTF-8 continuation and lead bytes).
bool isDisplayable(unsigned char c) {
  return c >= 0x20 || c == '\t' || c == '\n' || c == '\r';
}

/* Keeps a rendered payload on one line: the display format is one message per
   line, so a raw newline would break the columns. Any other control character
   becomes \xNN, which matters for `--format text` - that skips the printability
   check and would otherwise let a terminal escape sequence through. */
std::string escapeControlCharacters(std::string_view text) {
  static const char HEX_DIGITS[] = "0123456789abcdef";

  std::string out;
  out.reserve(text.size());
  for (const char c : text) {
    const auto byte = static_cast<unsigned char>(c);
    switch (c) {
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (byte < 0x20 || byte == 0x7f) {
          out += "\\x";
          out += HEX_DIGITS[byte >> 4];
          out += HEX_DIGITS[byte & 0x0f];
        } else {
          out += c;
        }
        break;
    }
  }
  return out;
}

std::string truncationNote(std::size_t total, std::size_t shown) {
  if (total <= shown) {
    return std::string();
  }
  return " … (+" + std::to_string(total - shown) + " bytes)";
}

}  // namespace

bool looksLikeText(const std::string& payload) {
  return std::all_of(payload.begin(), payload.end(), [](char c) { return isDisplayable(static_cast<unsigned char>(c)); });
}

std::string toHex(const std::string& bytes, int maxBytes) {
  static const char HEX_DIGITS[] = "0123456789abcdef";
  const std::size_t shown = std::min<std::size_t>(bytes.size(), static_cast<std::size_t>(maxBytes));

  std::string out;
  out.reserve(shown * 3 + 24);
  for (std::size_t i = 0; i < shown; ++i) {
    const auto c = static_cast<unsigned char>(bytes[i]);
    if (i > 0) {
      out += ' ';
    }
    out += HEX_DIGITS[c >> 4];
    out += HEX_DIGITS[c & 0x0f];
  }
  return out + truncationNote(bytes.size(), shown);
}

std::string renderPackedAny(const std::string& payload) {
  std::string_view valueBytes;
  const std::string_view claimedType = Wisp::AnyFrame::typeNameOf(payload, valueBytes);
  if (claimedType.empty()) {
    return std::string();
  }
  const std::string typeName(claimedType);
  const google::protobuf::Descriptor* descriptor = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(typeName);
  if (!descriptor) {
    return std::string();
  }

  const google::protobuf::Message* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
  if (!prototype) {
    return std::string();
  }

  std::unique_ptr<google::protobuf::Message> message(prototype->New());
  if (!message->ParseFromArray(valueBytes.data(), static_cast<int>(valueBytes.size()))) {
    return std::string();
  }

  google::protobuf::TextFormat::Printer printer;
  printer.SetSingleLineMode(true);
  std::string text;
  if (!printer.PrintToString(*message, &text)) {
    return std::string();
  }
  return typeName + " { " + text + "}";
}

std::string renderPayload(const std::string& payload, PayloadFormat format, int maxBytes) {
  if (payload.empty()) {
    return "(empty)";
  }

  if (format == PayloadFormat::Hex) {
    return toHex(payload, maxBytes);
  }

  if (format == PayloadFormat::Auto) {
    const std::string decoded = renderPackedAny(payload);
    if (!decoded.empty()) {
      // Decoded fields are worth more screen space than the same bytes in hex,
      // so the byte budget buys more characters here.
      const std::size_t budget = static_cast<std::size_t>(maxBytes) * DECODED_WIDTH_FACTOR;
      const std::size_t shown = std::min<std::size_t>(decoded.size(), budget);
      return escapeControlCharacters(std::string_view(decoded).substr(0, shown)) + (shown < decoded.size() ? " …" : "");
    }
    if (!looksLikeText(payload)) {
      return toHex(payload, maxBytes);
    }
  }

  const std::size_t shown = std::min<std::size_t>(payload.size(), static_cast<std::size_t>(maxBytes));
  return escapeControlCharacters(std::string_view(payload).substr(0, shown)) + truncationNote(payload.size(), shown);
}

}  // namespace WispCli
