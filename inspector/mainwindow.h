#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QAction>
#include <QDockWidget>
#include <QLabel>
#include <QLineEdit>
#include <QMainWindow>
#include <QMenu>
#include <QPushButton>
#include <QSet>
#include <QSplitter>
#include <QTableWidget>
#include <QTextEdit>
#include <QTreeWidget>
#include <QTreeWidgetItem>
#include <QVBoxLayout>

#include <deque>

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

private:
  void setupUi();
  void setupSysStatsView();

private:
  InspectorWorker* m_pWorker;
  ZmqWorker* m_pInjector;

  std::deque<InspectorPacket> m_packetHistory;

  QTableView* m_pPacketView;
  PacketTableModel* m_pTableModel;
  PacketFilterProxyModel* m_pProxyModel;

  QTreeWidget* m_pProtoTree;
  QTextEdit* m_pHexDump;

  QDockWidget* m_pStatsDock;

  QLabel* m_pBrokerIdLabel;
  QLabel* m_pUptimeLabel;
  QLabel* m_pClientsLabel;
  QLabel* m_pPeersLabel;
  QLabel* m_pMsgsSecLabel;
  QLabel* m_pKbSecLabel;
  QLabel* m_pTotalMsgsLabel;

  QLineEdit* m_pFilterBar;

  QSet<QString> m_knownTopics;
  QPushButton* m_pTopicFilterButton;
  QMenu* m_pTopicMenu;
};

#endif