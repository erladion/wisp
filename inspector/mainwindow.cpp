#include "mainwindow.h"

#include <QHeaderView>
#include <QScrollBar>
#include <QStringList>
#include <QStandardItemModel>
#include <QStatusBar>
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

  m_currentEndpoint = InspectorWorker::localTap();
  m_pWorker->setEndpoint(m_currentEndpoint);
  m_pWorker->start();

  // Listen for broker beacons so the selector can offer every broker on the
  // network. Listen-only by construction: an inspector that beaconed would be
  // dialed by brokers as if it were a peer broker.
  m_pBeaconListener = std::make_unique<beacon::Listener>(beacon::kDefaultPort, [this](const std::string& senderIp, const beacon::Beacon& heard) {
    // Hop to the UI thread before touching any widget or the broker map.
    const QString ip = QString::fromStdString(senderIp);
    QMetaObject::invokeMethod(
        this, [this, ip, heard]() { onBeaconHeard(ip, heard); }, Qt::QueuedConnection);
  });
  m_pBeaconListener->start();

  m_pBrokerExpiryTimer = new QTimer(this);
  connect(m_pBrokerExpiryTimer, &QTimer::timeout, this, &MainWindow::expireDiscoveredBrokers);
  m_pBrokerExpiryTimer->start(1000);

  ConnectionConfig config;
  config.address = "tcp://localhost:5555";
  config.clientId = "Inspector-Replay";

  m_pInjector = new ZmqWorker(config, nullptr, nullptr);
  m_pInjector->start();
}

MainWindow::~MainWindow() {
  if (m_pBeaconListener) {
    m_pBeaconListener->stop();  // joins before the callback's captured `this` dies
  }

  m_pWorker->stopWorker();
  m_pWorker->wait();

  m_pInjector->stop();
  delete m_pInjector;
}

void MainWindow::onBeaconHeard(const QString& senderIp, const beacon::Beacon& heard) {
  const QString uuid = QString::fromStdString(heard.uuid);

  DiscoveredBroker broker;
  broker.cluster = QString::fromStdString(heard.cluster);
  broker.uuid = uuid;
  broker.endpoint = heard.tapPort == 0 ? QString() : QString("tcp://%1:%2").arg(senderIp).arg(heard.tapPort);
  broker.lastSeen = std::chrono::steady_clock::now();

  const auto existing = m_discoveredBrokers.find(uuid);
  const bool changed = existing == m_discoveredBrokers.end() || existing->endpoint != broker.endpoint || existing->cluster != broker.cluster;

  m_discoveredBrokers.insert(uuid, broker);
  if (changed) {
    Logger::Log(Logger::INFO, "Discovered broker " + uuid.left(8).toStdString() + " (cluster '" + broker.cluster.toStdString() + "') at " +
                                  (broker.endpoint.isEmpty() ? std::string("no remote tap") : broker.endpoint.toStdString()));
    refreshBrokerList();
  }
}

void MainWindow::expireDiscoveredBrokers() {
  // Beacons are sent once a second; a broker silent for this long is gone
  // (or unreachable), so drop it from the selector.
  const auto cutoff = std::chrono::steady_clock::now() - std::chrono::seconds(10);

  bool removedAny = false;
  for (auto it = m_discoveredBrokers.begin(); it != m_discoveredBrokers.end();) {
    if (it->lastSeen < cutoff) {
      it = m_discoveredBrokers.erase(it);
      removedAny = true;
    } else {
      ++it;
    }
  }
  if (removedAny) {
    refreshBrokerList();
  }
}

void MainWindow::refreshBrokerList() {
  // Rebuilding resets the current index; remember what was selected so the
  // user's choice survives beacons arriving underneath them.
  const QString selected = m_currentEndpoint;

  QSignalBlocker blocker(m_pBrokerSelector);
  m_pBrokerSelector->clear();
  m_pBrokerSelector->addItem("Local broker (ipc)", InspectorWorker::localTap());

  for (const auto& broker : m_discoveredBrokers) {
    const QString shortUuid = broker.uuid.left(8);
    if (broker.endpoint.isEmpty()) {
      // Listed but not attachable: the broker was started without
      // --inspector-port, so it exposes no tap beyond its own machine.
      m_pBrokerSelector->addItem(QString("%1 · %2 — no remote tap").arg(broker.cluster, shortUuid), QString());
      if (auto* model = qobject_cast<QStandardItemModel*>(m_pBrokerSelector->model())) {
        if (auto* item = model->item(m_pBrokerSelector->count() - 1)) {
          item->setEnabled(false);
        }
      }
      continue;
    }
    m_pBrokerSelector->addItem(QString("%1 · %2 · %3").arg(broker.cluster, shortUuid, broker.endpoint), broker.endpoint);
  }

  for (int i = 0; i < m_pBrokerSelector->count(); ++i) {
    if (m_pBrokerSelector->itemData(i).toString() == selected) {
      m_pBrokerSelector->setCurrentIndex(i);
      return;
    }
  }
  // The attached broker just went away; keep showing its traffic but make the
  // selector reflect that it is no longer in the list.
  m_pBrokerSelector->setCurrentIndex(-1);
}

void MainWindow::onBrokerSelected(int index) {
  if (index < 0) {
    return;
  }
  const QString endpoint = m_pBrokerSelector->itemData(index).toString();
  if (endpoint.isEmpty()) {
    return;  // a "no remote tap" entry
  }
  attachTo(endpoint, m_pBrokerSelector->itemText(index));
}

void MainWindow::attachTo(const QString& endpoint, const QString& label) {
  if (endpoint == m_currentEndpoint) {
    return;
  }

  m_pWorker->stopWorker();
  m_pWorker->wait();

  clearCapture();

  m_pWorker->setEndpoint(endpoint);
  m_currentEndpoint = endpoint;
  m_pWorker->start();

  Logger::Log(Logger::INFO, "Inspector attached to " + endpoint.toStdString());
  statusBar()->showMessage("Attached to " + label, 5000);
}

// Traffic from a different broker must not be mixed into the same capture, so
// switching starts from a clean slate.
void MainWindow::clearCapture() {
  m_packetHistory.clear();
  m_pTableModel->historyCleared();

  m_knownTopics.clear();
  m_pTopicMenu->clear();

  m_pProtoTree->clear();
  m_pHexDump->clear();

  m_pBrokerIdLabel->setText("Broker ID: --");
  m_pClusterLabel->setText("Cluster: --");
  m_pUptimeLabel->setText("Uptime: -- s");
  m_pClientsLabel->setText("Clients: 0");
  m_pPeersLabel->setText("Peers: 0");
  m_pMsgsSecLabel->setText("Msgs/sec: 0");
  m_pKbSecLabel->setText("KB/sec: 0.00");
  m_pTotalMsgsLabel->setText("Total Msgs: 0");
  m_pDroppedLabel->setText("Dropped: 0");
  m_pDroppedLabel->setStyleSheet("");
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
      const QString cluster = QString::fromStdString(statsMsg.cluster());
      m_pClusterLabel->setText(QString("Cluster: %1").arg(cluster.isEmpty() ? "--" : cluster));
      m_pUptimeLabel->setText(QString("Uptime: %1 s").arg(statsMsg.uptime_sec()));

      m_pClientsLabel->setText(QString("Clients: %1").arg(statsMsg.clients_count()));
      m_pPeersLabel->setText(QString("Peers: %1").arg(statsMsg.peers_count()));

      m_pMsgsSecLabel->setText(QString("Msgs/sec: %1").arg(statsMsg.msgs_per_sec()));
      m_pKbSecLabel->setText(QString("KB/sec: %1").arg(statsMsg.kb_per_sec(), 0, 'f', 2));
      m_pTotalMsgsLabel->setText(QString("Total Msgs: %1").arg(statsMsg.total_msgs()));
      m_pDroppedLabel->setText(QString("Dropped: %1").arg(statsMsg.total_dropped()));
      m_pDroppedLabel->setStyleSheet(statsMsg.total_dropped() > 0 ? "color: #e74c3c; font-weight: bold;" : "");
    }
  }
}

void MainWindow::onSelectionChanged() {
  QModelIndexList selected = m_pPacketView->selectionModel()->selectedRows();
  if (selected.isEmpty()) {
    return;
  }

  // Bounds-checked: the history is trimmed from the front as it grows, so a
  // selection can outlive the row it pointed at.
  const int row = m_pProxyModel->mapToSource(selected.first()).row();
  if (row < 0 || static_cast<std::size_t>(row) >= m_packetHistory.size()) {
    return;
  }

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

  // Brokers found on the network, refreshed from their discovery beacons.
  m_pBrokerSelector = new QComboBox(this);
  m_pBrokerSelector->setMinimumWidth(320);
  m_pBrokerSelector->setToolTip(
      "Broker to inspect. Brokers appear here when they beacon on the LAN;\n"
      "attaching remotely requires the broker to run with --inspector-port.");
  m_pBrokerSelector->addItem("Local broker (ipc)", InspectorWorker::localTap());
  connect(m_pBrokerSelector, QOverload<int>::of(&QComboBox::currentIndexChanged), this, &MainWindow::onBrokerSelected);

  topBarLayout->addWidget(new QLabel("Broker:", this));
  topBarLayout->addWidget(m_pBrokerSelector);
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
  m_pClusterLabel = new QLabel("Cluster: --");
  m_pUptimeLabel = new QLabel("Uptime: -- s");
  m_pClientsLabel = new QLabel("Clients: 0");
  m_pPeersLabel = new QLabel("Peers: 0");
  m_pMsgsSecLabel = new QLabel("Msgs/sec: 0");
  m_pKbSecLabel = new QLabel("KB/sec: 0.00");
  m_pTotalMsgsLabel = new QLabel("Total Msgs: 0");
  m_pDroppedLabel = new QLabel("Dropped: 0");

  QFont boldFont("Monospace", 10, QFont::Bold);
  m_pBrokerIdLabel->setFont(boldFont);

  m_pMsgsSecLabel->setStyleSheet("color: #2ecc71; font-weight: bold;");  // Green

  layout->addWidget(m_pBrokerIdLabel);
  layout->addWidget(m_pClusterLabel);
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
  layout->addWidget(m_pDroppedLabel);

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

  const int row = m_pProxyModel->mapToSource(selectedRows.first()).row();
  if (row < 0 || static_cast<std::size_t>(row) >= m_packetHistory.size()) {
    return;
  }

  Envelope replayed;
  replayed.header = m_packetHistory[row].header;
  replayed.payload = m_packetHistory[row].payload;

  replayed.header.set_message_uuid(generateBinaryUUID());

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