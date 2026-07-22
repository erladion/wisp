#include <QApplication>

#include <absl/log/globals.h>

#include "mainwindow.h"

int main(int argc, char* argv[]) {
  // The inspector parses tap traffic the same way the broker parses the wire,
  // so it inherits the same exposure: protobuf logs straight to stderr on a
  // malformed frame, unthrottled. A remote TCP tap carries untrusted network
  // data, so silence that chatter here as the broker does. FATAL still gets
  // through.
  absl::SetMinLogLevel(absl::LogSeverityAtLeast::kFatal);

  QApplication a(argc, argv);

  MainWindow mw;
  mw.show();

  return a.exec();
}
