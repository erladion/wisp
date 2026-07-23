#include <QApplication>

// Whichever protobuf log-silencing API this build found; see the top-level
// CMakeLists. Neither exists across protobuf's abseil migration.
#if defined(WISP_HAVE_ABSL_LOG)
#include <absl/log/globals.h>
#elif defined(WISP_HAVE_PROTOBUF_LOG_SILENCER)
#include <google/protobuf/stubs/logging.h>
#endif

#include "mainwindow.h"

int main(int argc, char* argv[]) {
  /* The inspector parses tap traffic the same way the broker parses the wire,
     so it inherits the same exposure: protobuf logs straight to stderr on a
     malformed frame, unthrottled. A remote TCP tap carries untrusted network
     data, so silence that chatter here as the broker does. FATAL still gets
     through either way. */
#if defined(WISP_HAVE_ABSL_LOG)
  absl::SetMinLogLevel(absl::LogSeverityAtLeast::kFatal);
#elif defined(WISP_HAVE_PROTOBUF_LOG_SILENCER)
  // Suppresses non-fatal protobuf logging for as long as it is alive, so it has
  // to outlive the event loop below.
  const google::protobuf::LogSilencer protobufLogSilencer;
#endif

  QApplication a(argc, argv);

  MainWindow mw;
  mw.show();

  return a.exec();
}
