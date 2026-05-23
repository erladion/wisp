#include "mainwindow.h"

#include <QHeaderView>
#include <QScrollBar>
#include <QStringList>
#include <QVBoxLayout>

#include "hexutils.h"
#include "protoutils.h"

#include "messagekeys.h"

#include "config.h"
#include "uuidhelper.h"

#include "logger.h"

static QString formatByteSize(size_t bytes) {
  if (bytes < 1024) {
    return QString::number(bytes) + " B";
  }
  return QString::number(bytes / 1024.0, 'f', 2) + " KB";
}

MainWindow::MainWindow(QWidget* parent) : QMainWindow(parent) {
  qRegisterMetaType<InspectorPacket>("InspectorPacket");

  setupUi();

  m_worker = new InspectorWorker(this);
  connect(m_worker, &InspectorWorker::packetReceived, this, &MainWindow::onNewPacket, Qt::QueuedConnection);

  m_worker->start();

  ConnectionConfig config;
  config.address = "tcp://localhost:5555";
  config.clientId = "Inspector-Replay";

  m_injector = new ZmqWorker(config, nullptr, nullptr);
  m_injector->start();
}

MainWindow::~MainWindow() {
  m_worker->stopWorker();
  m_worker->wait();

  m_injector->stop();
  delete m_injector;
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

  size_t totalSize = packet.rawMemory.size();

  // Size of the specific 'google.protobuf.Any' inner payload
  size_t payloadSize = packet.parsedProto.payload().ByteSizeLong();

  QTableWidgetItem* msgSizeItem = new QTableWidgetItem(formatByteSize(totalSize));
  QTableWidgetItem* payloadSizeItem = new QTableWidgetItem(formatByteSize(payloadSize));

  msgSizeItem->setTextAlignment(Qt::AlignRight | Qt::AlignVCenter);
  payloadSizeItem->setTextAlignment(Qt::AlignRight | Qt::AlignVCenter);

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
    msgSizeItem->setBackground(brush);
    payloadSizeItem->setBackground(brush);
  }

  m_packetTable->setItem(row, 0, timeItem);
  m_packetTable->setItem(row, 1, senderItem);
  m_packetTable->setItem(row, 2, keyItem);
  m_packetTable->setItem(row, 3, topicItem);
  m_packetTable->setItem(row, 4, msgSizeItem);
  m_packetTable->setItem(row, 5, payloadSizeItem);

  QString qTopic = QString::fromStdString(packet.topic);
  if (qTopic.isEmpty()) {
    qTopic = "[Empty]";
  }

  if (!m_knownTopics.contains(qTopic)) {
    m_knownTopics.insert(qTopic);

    QAction* action = new QAction(qTopic, this);
    action->setCheckable(true);
    action->setChecked(true);

    m_topicMenu->addAction(action);

    connect(action, &QAction::toggled, this, &MainWindow::applyFilters);
  }

  applyFilters();

  if (isAtBottom) {
    m_packetTable->scrollToBottom();
  }
  if (packet.topic == Keys::SYS_STATS) {
    broker::SystemStats statsMsg;

    if (packet.parsedProto.payload().UnpackTo(&statsMsg)) {
      m_brokerIdLabel->setText(QString("Broker ID: %1").arg(QString::fromStdString(statsMsg.broker_id())));
      m_uptimeLabel->setText(QString("Uptime: %1 s").arg(statsMsg.uptime_sec()));

      m_clientsLabel->setText(QString("Clients: %1").arg(statsMsg.clients_count()));
      m_peersLabel->setText(QString("Peers: %1").arg(statsMsg.peers_count()));

      m_msgsSecLabel->setText(QString("Msgs/sec: %1").arg(statsMsg.msgs_per_sec()));
      m_kbSecLabel->setText(QString("KB/sec: %1").arg(statsMsg.kb_per_sec(), 0, 'f', 2));
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
  QWidget* centralWidget = new QWidget(this);
  QVBoxLayout* mainLayout = new QVBoxLayout(centralWidget);
  mainLayout->setContentsMargins(4, 4, 4, 4);

  QHBoxLayout* topBarLayout = new QHBoxLayout();

  m_filterBar = new QLineEdit(this);
  m_filterBar->setPlaceholderText("Filter by Topic, Key, or Sender...");
  m_filterBar->setClearButtonEnabled(true);

  connect(m_filterBar, &QLineEdit::textChanged, this, &MainWindow::applyFilters);

  m_topicFilterButton = new QPushButton("Topic Filters", this);
  m_topicMenu = new QMenu(this);
  m_topicFilterButton->setMenu(m_topicMenu);

  topBarLayout->addWidget(m_filterBar);
  topBarLayout->addWidget(m_topicFilterButton);

  QSplitter* mainSplitter = new QSplitter(Qt::Vertical, this);

  m_packetTable = new QTableWidget(0, 6, this);
  m_packetTable->setHorizontalHeaderLabels({"Time", "Sender", "Key", "Topic", "Msg size", "Payload size"});

  QHeaderView* header = m_packetTable->horizontalHeader();
  header->setSectionResizeMode(0, QHeaderView::ResizeToContents);  // Time
  header->setSectionResizeMode(1, QHeaderView::Stretch);           // Sender
  header->setSectionResizeMode(2, QHeaderView::ResizeToContents);  // Key
  header->setSectionResizeMode(3, QHeaderView::ResizeToContents);  // Topic
  header->setSectionResizeMode(4, QHeaderView::ResizeToContents);  // Msg Size
  header->setSectionResizeMode(5, QHeaderView::ResizeToContents);  // Payload Size
  header->setStretchLastSection(false);

  m_packetTable->setSelectionBehavior(QAbstractItemView::SelectRows);
  m_packetTable->setSelectionMode(QAbstractItemView::SingleSelection);
  connect(m_packetTable, &QTableWidget::itemSelectionChanged, this, &MainWindow::onSelectionChanged);

  m_packetTable->setContextMenuPolicy(Qt::CustomContextMenu);
  connect(m_packetTable, &QTableWidget::customContextMenuRequested, this, &MainWindow::showContextMenu);

  m_protoTree = new QTreeWidget(this);
  m_protoTree->setHeaderLabels({"Field", "Value"});

  m_hexDump = new QTextEdit(this);
  m_hexDump->setFontFamily("Courier");
  m_hexDump->setReadOnly(true);

  mainSplitter->addWidget(m_packetTable);
  mainSplitter->addWidget(m_protoTree);
  mainSplitter->addWidget(m_hexDump);

  mainLayout->addLayout(topBarLayout);
  mainLayout->addWidget(mainSplitter);

  setCentralWidget(centralWidget);
  resize(1024, 768);

  setupSysStatsView();
}

void MainWindow::setupSysStatsView() {
  m_statsDock = new QDockWidget("Live System Stats", this);

  m_statsDock->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);

  QWidget* dockContent = new QWidget();
  QVBoxLayout* layout = new QVBoxLayout(dockContent);

  m_brokerIdLabel = new QLabel("Broker ID: --");
  m_brokerIdLabel->setWordWrap(true);
  m_brokerIdLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
  m_uptimeLabel = new QLabel("Uptime: -- s");
  m_clientsLabel = new QLabel("Clients: 0");
  m_peersLabel = new QLabel("Peers: 0");
  m_msgsSecLabel = new QLabel("Msgs/sec: 0");
  m_kbSecLabel = new QLabel("KB/sec: 0.00");
  m_totalMsgsLabel = new QLabel("Total Msgs: 0");

  QFont boldFont("Courier", 10, QFont::Bold);
  m_brokerIdLabel->setFont(boldFont);

  m_msgsSecLabel->setStyleSheet("color: #2ecc71; font-weight: bold;");  // Green

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

  layout->addStretch();

  m_statsDock->setWidget(dockContent);
  addDockWidget(Qt::RightDockWidgetArea, m_statsDock);
}

void MainWindow::applyFilters() {
  const QString lowerText = m_filterBar->text().toLower();

  QSet<QString> allowedTopics;
  for (QAction* action : m_topicMenu->actions()) {
    if (action->isChecked()) {
      allowedTopics.insert(action->text());
    }
  }

  for (int i = 0; i < m_packetTable->rowCount(); ++i) {
    const QString sender = m_packetTable->item(i, 1)->text().toLower();
    const QString key = m_packetTable->item(i, 2)->text().toLower();
    const QString topic = m_packetTable->item(i, 3)->text();  // Keep case for exact match

    const bool textMatch = lowerText.isEmpty() || sender.contains(lowerText) || key.contains(lowerText) || topic.toLower().contains(lowerText);
    const bool topicMatch = allowedTopics.contains(topic);

    m_packetTable->setRowHidden(i, !(textMatch && topicMatch));
  }
}

void MainWindow::showContextMenu(const QPoint& pos) {
  QList<QTableWidgetItem*> selectedItems = m_packetTable->selectedItems();
  if (selectedItems.isEmpty()) {
    return;
  }

  QMenu menu(this);
  QAction* replayAction = menu.addAction("Replay Message");

  connect(replayAction, &QAction::triggered, this, &MainWindow::replaySelectedMessage);
  menu.exec(m_packetTable->viewport()->mapToGlobal(pos));
}

void MainWindow::replaySelectedMessage() {
  QList<QTableWidgetItem*> selectedItems = m_packetTable->selectedItems();
  if (selectedItems.isEmpty()) {
    return;
  }

  int row = selectedItems.first()->row();
  if (row >= m_packetHistory.size()) {
    return;
  }

  broker::BrokerPayload replayedMsg = m_packetHistory[row].parsedProto;

  replayedMsg.set_message_uuid(generateUUID());

  std::string originalSender = replayedMsg.sender_id();
  if (originalSender.find("REPLAY_") == std::string::npos) {
    replayedMsg.set_sender_id("REPLAY_" + originalSender);
  }

  if (Keys::isControlMessage(replayedMsg.handler_key())) {
    m_injector->writeControlMessage(replayedMsg);
  } else {
    m_injector->writeMessage(replayedMsg);
  }

  Logger::Log(Logger::INFO, "Injected replay for topic: " + replayedMsg.topic());
}
