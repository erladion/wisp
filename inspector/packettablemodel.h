#ifndef PACKETTABLEMODEL_H
#define PACKETTABLEMODEL_H

#include <QAbstractTableModel>
#include <QSortFilterProxyModel>

#include <deque>

#include "datamodel.h"

class PacketTableModel : public QAbstractTableModel {
  Q_OBJECT
public:
  PacketTableModel(const std::deque<InspectorPacket>& history, QObject* parent = nullptr);
  int rowCount(const QModelIndex& parent = QModelIndex()) const override;
  int columnCount(const QModelIndex& parent = QModelIndex()) const override;
  QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override;
  QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override;

  void packetAdded();
  // The backing history was cleared wholesale (e.g. switching brokers).
  void historyCleared();  // Call this when the history grows

  // Bracket the removal of the oldest `count` packets: call
  // packetsAboutToBeTrimmed(), erase from the front of the history deque,
  // then packetsTrimmed(). Qt requires the begin/end pair to surround the
  // actual mutation.
  void packetsAboutToBeTrimmed(int count);
  void packetsTrimmed();

private:
  const std::deque<InspectorPacket>& m_history;
};

// 2. THE HIGH-SPEED FILTER
class PacketFilterProxyModel : public QSortFilterProxyModel {
  Q_OBJECT
public:
  PacketFilterProxyModel(QObject* parent = nullptr);

  QSet<QString> allowedTopics;
  QString searchText;

  void updateFilters(const QString& text, const QSet<QString>& topics);

protected:
  bool filterAcceptsRow(int sourceRow, const QModelIndex& sourceParent) const override;
};
#endif  // PACKETTABLEMODEL_H
