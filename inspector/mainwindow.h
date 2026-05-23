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

#include <vector>
#include "inspectorworker.h"
#include "zmqworker.h"

class MainWindow : public QMainWindow {
  Q_OBJECT

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
  InspectorWorker* m_worker;
  ZmqWorker* m_injector;

  std::vector<InspectorPacket> m_packetHistory;

  QTableWidget* m_packetTable;
  QTreeWidget* m_protoTree;
  QTextEdit* m_hexDump;

  QDockWidget* m_statsDock;

  QLabel* m_brokerIdLabel;
  QLabel* m_uptimeLabel;
  QLabel* m_clientsLabel;
  QLabel* m_peersLabel;
  QLabel* m_msgsSecLabel;
  QLabel* m_kbSecLabel;
  QLabel* m_totalMsgsLabel;

  QLineEdit* m_filterBar;

  QSet<QString> m_knownTopics;
  QPushButton* m_topicFilterButton;
  QMenu* m_topicMenu;
};

#endif