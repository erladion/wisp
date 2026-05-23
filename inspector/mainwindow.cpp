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
  QScrollBar* scrollBar = m_packetView->verticalScrollBar();
  bool isAtBottom = (scrollBar->value() == scrollBar->maximum());

  m_packetHistory.push_back(packet);
  m_tableModel->packetAdded();

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
    applyFilters();  // Update if a new topic appears
  }

  if (isAtBottom) {
    m_packetView->scrollToBottom();
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
  QModelIndexList selected = m_packetView->selectionModel()->selectedRows();
  if (selected.isEmpty()) {
    return;
  }

  int row = m_proxyModel->mapToSource(selected.first()).row();

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

  m_packetView = new QTableView(this);
  m_tableModel = new PacketTableModel(m_packetHistory, this);
  m_proxyModel = new PacketFilterProxyModel(this);

  m_proxyModel->setSourceModel(m_tableModel);
  m_packetView->setModel(m_proxyModel);

  QHeaderView* header = m_packetView->horizontalHeader();
  header->setSectionResizeMode(0, QHeaderView::ResizeToContents);  // Time
  header->setSectionResizeMode(1, QHeaderView::Stretch);           // Sender
  header->setSectionResizeMode(2, QHeaderView::ResizeToContents);  // Key
  header->setSectionResizeMode(3, QHeaderView::ResizeToContents);  // Topic
  header->setSectionResizeMode(4, QHeaderView::ResizeToContents);  // Msg Size
  header->setSectionResizeMode(5, QHeaderView::ResizeToContents);  // Payload Size
  header->setStretchLastSection(false);

  m_packetView->setSelectionBehavior(QAbstractItemView::SelectRows);
  m_packetView->setSelectionMode(QAbstractItemView::SingleSelection);
  connect(m_packetView->selectionModel(), &QItemSelectionModel::selectionChanged, this, &MainWindow::onSelectionChanged);

  m_packetView->setContextMenuPolicy(Qt::CustomContextMenu);
  connect(m_packetView, &QTableView::customContextMenuRequested, this, &MainWindow::showContextMenu);

  m_protoTree = new QTreeWidget(this);
  m_protoTree->setHeaderLabels({"Field", "Value"});

  m_hexDump = new QTextEdit(this);
  m_hexDump->setFontFamily("Monospace");
  m_hexDump->setReadOnly(true);

  mainSplitter->addWidget(m_packetView);
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

  QFont boldFont("Monospace", 10, QFont::Bold);
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
  QSet<QString> allowedTopics;
  for (QAction* action : m_topicMenu->actions()) {
    if (action->isChecked())
      allowedTopics.insert(action->text());
  }

  m_proxyModel->updateFilters(m_filterBar->text(), allowedTopics);
}

void MainWindow::showContextMenu(const QPoint& pos) {
  QModelIndexList selectedRows = m_packetView->selectionModel()->selectedRows();
  if (selectedRows.isEmpty()) {
    return;
  }

  QMenu menu(this);
  QAction* replayAction = menu.addAction("Replay Message");

  connect(replayAction, &QAction::triggered, this, &MainWindow::replaySelectedMessage);

  menu.exec(m_packetView->viewport()->mapToGlobal(pos));
}

void MainWindow::replaySelectedMessage() {
  QModelIndexList selectedRows = m_packetView->selectionModel()->selectedRows();
  if (selectedRows.isEmpty()) {
    return;
  }

  int row = m_proxyModel->mapToSource(selectedRows.first()).row();

  if (row >= m_packetHistory.size() || row < 0) {
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