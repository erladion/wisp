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

  setWindowTitle("Wisp Inspector");
  setupUi();

  m_pWorker = new InspectorWorker(this);
  connect(m_pWorker, &InspectorWorker::packetReceived, this, &MainWindow::onNewPacket, Qt::QueuedConnection);

  m_pWorker->start();

  ConnectionConfig config;
  config.address = "tcp://localhost:5555";
  config.clientId = "Inspector-Replay";

  m_pInjector = new ZmqWorker(config, nullptr, nullptr);
  m_pInjector->start();
}

MainWindow::~MainWindow() {
  m_pWorker->stopWorker();
  m_pWorker->wait();

  m_pInjector->stop();
  delete m_pInjector;
}

void MainWindow::onNewPacket(const InspectorPacket& packet) {
  QScrollBar* scrollBar = m_pPacketView->verticalScrollBar();
  bool isAtBottom = (scrollBar->value() == scrollBar->maximum());

  m_packetHistory.push_back(packet);
  m_pTableModel->packetAdded();

  if (m_packetHistory.size() > MaxPacketHistory) {
    m_pTableModel->packetsAboutToBeTrimmed(TrimChunk);
    m_packetHistory.erase(m_packetHistory.begin(), m_packetHistory.begin() + TrimChunk);
    m_pTableModel->packetsTrimmed();
  }

  QString qTopic = QString::fromStdString(packet.topic);
  if (qTopic.isEmpty()) {
    qTopic = "[Empty]";
  }

  if (!m_knownTopics.contains(qTopic)) {
    m_knownTopics.insert(qTopic);
    QAction* action = new QAction(qTopic, this);
    action->setCheckable(true);
    action->setChecked(true);
    m_pTopicMenu->addAction(action);
    connect(action, &QAction::toggled, this, &MainWindow::applyFilters);
    applyFilters();  // Update if a new topic appears
  }

  if (isAtBottom) {
    m_pPacketView->scrollToBottom();
  }

  if (packet.topic == Keys::SYS_STATS) {
    broker::SystemStats statsMsg;

    google::protobuf::Any any;
    if (any.ParseFromString(packet.payload) && any.UnpackTo(&statsMsg)) {
      m_pBrokerIdLabel->setText(QString("Broker ID: %1").arg(QString::fromStdString(statsMsg.broker_id())));
      m_pUptimeLabel->setText(QString("Uptime: %1 s").arg(statsMsg.uptime_sec()));

      m_pClientsLabel->setText(QString("Clients: %1").arg(statsMsg.clients_count()));
      m_pPeersLabel->setText(QString("Peers: %1").arg(statsMsg.peers_count()));

      m_pMsgsSecLabel->setText(QString("Msgs/sec: %1").arg(statsMsg.msgs_per_sec()));
      m_pKbSecLabel->setText(QString("KB/sec: %1").arg(statsMsg.kb_per_sec(), 0, 'f', 2));
      m_pTotalMsgsLabel->setText(QString("Total Msgs: %1").arg(statsMsg.total_msgs()));
    }
  }
}

void MainWindow::onSelectionChanged() {
  QModelIndexList selected = m_pPacketView->selectionModel()->selectedRows();
  if (selected.isEmpty()) {
    return;
  }

  int row = m_pProxyModel->mapToSource(selected.first()).row();

  const InspectorPacket& packet = m_packetHistory[row];
  m_pHexDump->setPlainText(QString::fromStdString(HexUtils::generateHexDump(packet.payload)));
  m_pProtoTree->clear();
  ProtoUtils::drawEnvelopeAndPayload(packet.header, packet.payload, m_pProtoTree);
}

void MainWindow::setupUi() {
  QWidget* centralWidget = new QWidget(this);
  QVBoxLayout* mainLayout = new QVBoxLayout(centralWidget);
  mainLayout->setContentsMargins(4, 4, 4, 4);

  QHBoxLayout* topBarLayout = new QHBoxLayout();

  m_pFilterBar = new QLineEdit(this);
  m_pFilterBar->setPlaceholderText("Filter by Topic, Key, or Sender...");
  m_pFilterBar->setClearButtonEnabled(true);

  connect(m_pFilterBar, &QLineEdit::textChanged, this, &MainWindow::applyFilters);

  m_pTopicFilterButton = new QPushButton("Topic Filters", this);
  m_pTopicMenu = new QMenu(this);
  m_pTopicFilterButton->setMenu(m_pTopicMenu);

  topBarLayout->addWidget(m_pFilterBar);
  topBarLayout->addWidget(m_pTopicFilterButton);

  QSplitter* mainSplitter = new QSplitter(Qt::Vertical, this);

  m_pPacketView = new QTableView(this);
  m_pTableModel = new PacketTableModel(m_packetHistory, this);
  m_pProxyModel = new PacketFilterProxyModel(this);

  m_pProxyModel->setSourceModel(m_pTableModel);
  m_pPacketView->setModel(m_pProxyModel);

  QHeaderView* header = m_pPacketView->horizontalHeader();
  header->setSectionResizeMode(0, QHeaderView::ResizeToContents);  // Time
  header->setSectionResizeMode(1, QHeaderView::Stretch);           // Sender
  header->setSectionResizeMode(2, QHeaderView::ResizeToContents);  // Key
  header->setSectionResizeMode(3, QHeaderView::ResizeToContents);  // Topic
  header->setSectionResizeMode(4, QHeaderView::ResizeToContents);  // Msg Size
  header->setSectionResizeMode(5, QHeaderView::ResizeToContents);  // Payload Size
  header->setStretchLastSection(false);

  m_pPacketView->setSelectionBehavior(QAbstractItemView::SelectRows);
  m_pPacketView->setSelectionMode(QAbstractItemView::SingleSelection);
  connect(m_pPacketView->selectionModel(), &QItemSelectionModel::selectionChanged, this, &MainWindow::onSelectionChanged);

  m_pPacketView->setContextMenuPolicy(Qt::CustomContextMenu);
  connect(m_pPacketView, &QTableView::customContextMenuRequested, this, &MainWindow::showContextMenu);

  m_pProtoTree = new QTreeWidget(this);
  m_pProtoTree->setHeaderLabels({"Field", "Value"});

  m_pHexDump = new QTextEdit(this);
  m_pHexDump->setFontFamily("Monospace");
  m_pHexDump->setReadOnly(true);

  mainSplitter->addWidget(m_pPacketView);
  mainSplitter->addWidget(m_pProtoTree);
  mainSplitter->addWidget(m_pHexDump);

  mainLayout->addLayout(topBarLayout);
  mainLayout->addWidget(mainSplitter);

  setCentralWidget(centralWidget);
  resize(1024, 768);

  setupSysStatsView();
}

void MainWindow::setupSysStatsView() {
  m_pStatsDock = new QDockWidget("Live System Stats", this);

  m_pStatsDock->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);

  QWidget* dockContent = new QWidget();
  QVBoxLayout* layout = new QVBoxLayout(dockContent);

  m_pBrokerIdLabel = new QLabel("Broker ID: --");
  m_pBrokerIdLabel->setWordWrap(true);
  m_pBrokerIdLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
  m_pUptimeLabel = new QLabel("Uptime: -- s");
  m_pClientsLabel = new QLabel("Clients: 0");
  m_pPeersLabel = new QLabel("Peers: 0");
  m_pMsgsSecLabel = new QLabel("Msgs/sec: 0");
  m_pKbSecLabel = new QLabel("KB/sec: 0.00");
  m_pTotalMsgsLabel = new QLabel("Total Msgs: 0");

  QFont boldFont("Monospace", 10, QFont::Bold);
  m_pBrokerIdLabel->setFont(boldFont);

  m_pMsgsSecLabel->setStyleSheet("color: #2ecc71; font-weight: bold;");  // Green

  layout->addWidget(m_pBrokerIdLabel);
  layout->addWidget(m_pUptimeLabel);

  QFrame* line1 = new QFrame();
  line1->setFrameShape(QFrame::HLine);
  layout->addWidget(line1);

  layout->addWidget(m_pClientsLabel);
  layout->addWidget(m_pPeersLabel);

  QFrame* line2 = new QFrame();
  line2->setFrameShape(QFrame::HLine);
  layout->addWidget(line2);

  layout->addWidget(m_pMsgsSecLabel);
  layout->addWidget(m_pKbSecLabel);
  layout->addWidget(m_pTotalMsgsLabel);

  layout->addStretch();

  m_pStatsDock->setWidget(dockContent);
  addDockWidget(Qt::RightDockWidgetArea, m_pStatsDock);
}

void MainWindow::applyFilters() {
  QSet<QString> allowedTopics;
  for (QAction* action : m_pTopicMenu->actions()) {
    if (action->isChecked()) {
      allowedTopics.insert(action->text());
    }
  }

  m_pProxyModel->updateFilters(m_pFilterBar->text(), allowedTopics);
}

void MainWindow::showContextMenu(const QPoint& pos) {
  QModelIndexList selectedRows = m_pPacketView->selectionModel()->selectedRows();
  if (selectedRows.isEmpty()) {
    return;
  }

  QMenu menu(this);
  QAction* replayAction = menu.addAction("Replay Message");

  connect(replayAction, &QAction::triggered, this, &MainWindow::replaySelectedMessage);

  menu.exec(m_pPacketView->viewport()->mapToGlobal(pos));
}

void MainWindow::replaySelectedMessage() {
  QModelIndexList selectedRows = m_pPacketView->selectionModel()->selectedRows();
  if (selectedRows.isEmpty()) {
    return;
  }

  int row = m_pProxyModel->mapToSource(selectedRows.first()).row();

  if (row >= m_packetHistory.size() || row < 0) {
    return;
  }

  Envelope replayed;
  replayed.header = m_packetHistory[row].header;
  replayed.payload = m_packetHistory[row].payload;

  replayed.header.set_message_uuid(generateUUID());

  std::string originalSender = replayed.header.sender_id();
  if (originalSender.find("REPLAY_") == std::string::npos) {
    replayed.header.set_sender_id("REPLAY_" + originalSender);
  }

  if (Keys::isControlMessage(replayed.header.handler_key())) {
    m_pInjector->writeControlMessage(replayed);
  } else {
    m_pInjector->writeMessage(replayed);
  }

  Logger::Log(Logger::INFO, "Injected replay for topic: " + replayed.header.topic());
}