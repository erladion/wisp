#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QAction>
#include <QComboBox>
#include <QDockWidget>
#include <QLabel>
#include <QLineEdit>
#include <QMainWindow>
#include <QMap>
#include <QMenu>
#include <QPushButton>
#include <QSet>
#include <QSplitter>
#include <QTableWidget>
#include <QTextEdit>
#include <QTimer>
#include <QTreeWidget>
#include <QTreeWidgetItem>
#include <QVBoxLayout>

#include <chrono>
#include <deque>
#include <memory>

#include "beacon.h"
#include "inspectorworker.h"
#include "zmqworker.h"

#include "packettablemodel.h"

class MainWindow : public QMainWindow {
  Q_OBJECT

  // Bound on the in-memory capture: above this the oldest packets are dropped
  // in batches (batching amortizes the vector erase and table-model churn).
  static constexpr size_t MaxPacketHistory = 100000;
  static constexpr size_t TrimChunk = 10000;

public:
  MainWindow(QWidget* parent = nullptr);
  ~MainWindow();

private slots:
  void applyFilters();
  void onSelectionChanged();
  void onNewPacket(const InspectorPacket& packet);

  void showContextMenu(const QPoint& pos);
  void replaySelectedMessage();

  void onBrokerSelected(int index);
  void expireDiscoveredBrokers();

private:
  void setupUi();
  void setupSysStatsView();

  // A broker heard on the network. Only those advertising a tap port can be
  // attached to; the rest are listed (disabled) so it is obvious they exist
  // and why they cannot be selected.
  struct DiscoveredBroker {
    QString cluster;
    QString uuid;
    QString endpoint;        // empty when the broker exposes no remote tap
    QString routerEndpoint;  // where the broker's clients connect, from its beacon
    std::chrono::steady_clock::time_point lastSeen;
  };

  void onBeaconHeard(const QString& senderIp, const beacon::Beacon& heard);
  void refreshBrokerList();
  void attachTo(const QString& endpoint, const QString& label);
  void clearCapture();
  void retargetInjector(const QString& address);
  QString routerEndpointForTap(const QString& tapEndpoint) const;

private:
  InspectorWorker* m_pWorker;
  std::unique_ptr<ZmqWorker> m_pInjector;
  QString m_injectorAddress;

  std::deque<InspectorPacket> m_packetHistory;

  QTableView* m_pPacketView;
  PacketTableModel* m_pTableModel;
  PacketFilterProxyModel* m_pProxyModel;

  QTreeWidget* m_pProtoTree;
  QTextEdit* m_pHexDump;

  QDockWidget* m_pStatsDock;

  QLabel* m_pBrokerIdLabel;
  QLabel* m_pClusterLabel;
  QLabel* m_pUptimeLabel;
  QLabel* m_pClientsLabel;
  QLabel* m_pPeersLabel;
  QLabel* m_pMsgsSecLabel;
  QLabel* m_pKbSecLabel;
  QLabel* m_pTotalMsgsLabel;
  QLabel* m_pDroppedLabel;

  QLineEdit* m_pFilterBar;

  QSet<QString> m_knownTopics;
  QPushButton* m_pTopicFilterButton;
  QMenu* m_pTopicMenu;

  // Broker discovery: beacons arrive on the listener's own thread and are
  // marshalled onto the UI thread before touching any of this state.
  std::unique_ptr<beacon::Listener> m_pBeaconListener;
  QMap<QString, DiscoveredBroker> m_discoveredBrokers;  // keyed by uuid
  QComboBox* m_pBrokerSelector;
  QTimer* m_pBrokerExpiryTimer;
  QString m_currentEndpoint;
};

#endif