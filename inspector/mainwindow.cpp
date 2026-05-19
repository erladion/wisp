#include "mainwindow.h"

#include <QHeaderView>
#include <QScrollBar>
#include <QStringList>
#include <QVBoxLayout>

#include "hexutils.h"
#include "protoutils.h"

#include "messagekeys.h"

MainWindow::MainWindow(QWidget* parent) : QMainWindow(parent) {
  // 1. Tell Qt about our custom struct for signals
  qRegisterMetaType<InspectorPacket>("InspectorPacket");

  setupUi();

  // 2. Spin up the ZeroMQ background thread
  m_worker = new InspectorWorker(this);

  // 3. Connect the threads safely
  connect(m_worker, &InspectorWorker::packetReceived, this, &MainWindow::onNewPacket, Qt::QueuedConnection);

  m_worker->start();
}

MainWindow::~MainWindow() {
  m_worker->stopWorker();
  m_worker->wait();
}

void MainWindow::onNewPacket(const InspectorPacket& packet) {
  m_packetHistory.push_back(packet);

  QScrollBar* scrollBar = m_packetTable->verticalScrollBar();
  bool isAtBottom = (scrollBar->value() == scrollBar->maximum());

  int row = m_packetTable->rowCount();
  m_packetTable->insertRow(row);

  QTableWidgetItem* timeItem = new QTableWidgetItem(QString::fromStdString(packet.timestamp));
  QTableWidgetItem* senderItem = new QTableWidgetItem(QString::fromStdString(packet.senderId));
  QTableWidgetItem* keyItem = new QTableWidgetItem(QString::fromStdString(packet.key));
  QTableWidgetItem* topicItem = new QTableWidgetItem(QString::fromStdString(packet.topic));

  QColor rowColor;
  if (Keys::isControlMessage(packet.key)) {
    rowColor = QColor(40, 60, 255, 100);
  } else if (packet.key == Keys::SYS_STATS) {
    rowColor = QColor(128, 128, 0, 100);
  } else {
    // Normal data messages get the default color (or explicitly set one)
    // rowColor = QColor(50, 50, 50); // Optional: explicitly set normal row color
  }

  if (rowColor.isValid()) {
    QBrush brush(rowColor);
    timeItem->setBackground(brush);
    senderItem->setBackground(brush);
    keyItem->setBackground(brush);
    topicItem->setBackground(brush);
  }

  m_packetTable->setItem(row, 0, timeItem);
  m_packetTable->setItem(row, 1, senderItem);
  m_packetTable->setItem(row, 2, keyItem);
  m_packetTable->setItem(row, 3, topicItem);

  if (isAtBottom) {
    m_packetTable->scrollToBottom();
  }

  if (packet.topic == Keys::SYS_STATS) {
    broker::SystemStats statsMsg;

    if (packet.parsedProto.payload().UnpackTo(&statsMsg)) {
      // Update all labels
      m_brokerIdLabel->setText(QString("Broker ID: %1").arg(QString::fromStdString(statsMsg.broker_id())));
      m_uptimeLabel->setText(QString("Uptime: %1 s").arg(statsMsg.uptime_sec()));

      m_clientsLabel->setText(QString("Clients: %1").arg(statsMsg.clients_count()));
      m_peersLabel->setText(QString("Peers: %1").arg(statsMsg.peers_count()));

      m_msgsSecLabel->setText(QString("Msgs/sec: %1").arg(statsMsg.msgs_per_sec()));
      m_kbSecLabel->setText(QString("KB/sec: %1").arg(statsMsg.kb_per_sec(), 0, 'f', 2));  // 2 decimal places
      m_totalMsgsLabel->setText(QString("Total Msgs: %1").arg(statsMsg.total_msgs()));
    }
  }
}

void MainWindow::onSelectionChanged() {
  QList<QTableWidgetItem*> selectedItems = m_packetTable->selectedItems();

  if (selectedItems.isEmpty()) {
    return;
  }

  int row = selectedItems.first()->row();

  if (row >= m_packetHistory.size()) {
    return;
  }

  const InspectorPacket& packet = m_packetHistory[row];

  m_hexDump->setPlainText(QString::fromStdString(HexUtils::generateHexDump(packet.rawMemory)));

  m_protoTree->clear();
  ProtoUtils::drawEnvelopeAndPayload(packet.parsedProto, m_protoTree);
}

void MainWindow::setupUi() {
  QSplitter* mainSplitter = new QSplitter(Qt::Vertical, this);

  m_packetTable = new QTableWidget(0, 4, this);
  m_packetTable->setHorizontalHeaderLabels({"Time", "Sender", "Key", "Topic"});
  m_packetTable->horizontalHeader()->setStretchLastSection(true);
  m_packetTable->setSelectionBehavior(QAbstractItemView::SelectRows);
  m_packetTable->setSelectionMode(QAbstractItemView::SingleSelection);
  connect(m_packetTable, &QTableWidget::itemSelectionChanged, this, &MainWindow::onSelectionChanged);
  m_protoTree = new QTreeWidget(this);
  m_protoTree->setHeaderLabels({"Field", "Value"});

  m_hexDump = new QTextEdit(this);
  m_hexDump->setFontFamily("Courier");  // Monospace for hex
  m_hexDump->setReadOnly(true);

  mainSplitter->addWidget(m_packetTable);
  mainSplitter->addWidget(m_protoTree);
  mainSplitter->addWidget(m_hexDump);

  setCentralWidget(mainSplitter);
  resize(1024, 768);

  setupSysStatsView();
}

void MainWindow::setupSysStatsView() {
  // 1. Create the Docking Window
  m_statsDock = new QDockWidget("Live System Stats", this);

  // Lock it to the left or right sides so it doesn't mess up your top/bottom layout
  m_statsDock->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);

  // 2. Create the container widget and layout
  QWidget* dockContent = new QWidget();
  QVBoxLayout* layout = new QVBoxLayout(dockContent);

  // 3. Initialize the labels
  m_brokerIdLabel = new QLabel("Broker ID: --");
  m_brokerIdLabel->setWordWrap(true);
  m_brokerIdLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
  m_uptimeLabel = new QLabel("Uptime: -- s");
  m_clientsLabel = new QLabel("Clients: 0");
  m_peersLabel = new QLabel("Peers: 0");
  m_msgsSecLabel = new QLabel("Msgs/sec: 0");
  m_kbSecLabel = new QLabel("KB/sec: 0.00");
  m_totalMsgsLabel = new QLabel("Total Msgs: 0");

  // Optional: Style the high-priority metrics so they stand out
  QFont boldFont("Courier", 10, QFont::Bold);
  m_brokerIdLabel->setFont(boldFont);

  m_msgsSecLabel->setStyleSheet("color: #2ecc71; font-weight: bold;");  // Green

  // 4. Add them to the layout with some spacing lines
  layout->addWidget(m_brokerIdLabel);
  layout->addWidget(m_uptimeLabel);

  QFrame* line1 = new QFrame();
  line1->setFrameShape(QFrame::HLine);
  layout->addWidget(line1);

  layout->addWidget(m_clientsLabel);
  layout->addWidget(m_peersLabel);

  QFrame* line2 = new QFrame();
  line2->setFrameShape(QFrame::HLine);
  layout->addWidget(line2);

  layout->addWidget(m_msgsSecLabel);
  layout->addWidget(m_kbSecLabel);
  layout->addWidget(m_totalMsgsLabel);

  // Push everything to the top
  layout->addStretch();

  // 5. Attach to Dock and add to Main Window
  m_statsDock->setWidget(dockContent);
  addDockWidget(Qt::RightDockWidgetArea, m_statsDock);

  // (Optional) If you have a QMenu or QToolBar, you can add the toggle action:
  // ui->menuView->addAction(m_statsDock->toggleViewAction());
}