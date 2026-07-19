#include "packettablemodel.h"

#include <QColor>

#include "messagekeys.h"

static QString formatByteSize(size_t bytes) {
  if (bytes < 1024) {
    return QString::number(bytes) + " B";
  }
  return QString::number(bytes / 1024.0, 'f', 2) + " KB";
}

PacketTableModel::PacketTableModel(const std::deque<InspectorPacket>& history, QObject* parent) : QAbstractTableModel(parent), m_history(history) {}

int PacketTableModel::rowCount(const QModelIndex&) const {
  return m_history.size();
}
int PacketTableModel::columnCount(const QModelIndex&) const {
  return 6;
}

void PacketTableModel::packetAdded() {
  beginInsertRows(QModelIndex(), m_history.size() - 1, m_history.size() - 1);
  endInsertRows();
}

void PacketTableModel::historyCleared() {
  beginResetModel();
  endResetModel();
}

void PacketTableModel::packetsAboutToBeTrimmed(int count) {
  beginRemoveRows(QModelIndex(), 0, count - 1);
}

void PacketTableModel::packetsTrimmed() {
  endRemoveRows();
}

QVariant PacketTableModel::headerData(int section, Qt::Orientation orientation, int role) const {
  if (role != Qt::DisplayRole || orientation != Qt::Horizontal) {
    return QVariant();
  }
  const char* headers[] = {"Time", "Sender", "Key", "Topic", "Msg size", "Payload size"};
  return headers[section];
}

QVariant PacketTableModel::data(const QModelIndex& index, int role) const {
  if (!index.isValid() || index.row() < 0 || static_cast<std::size_t>(index.row()) >= m_history.size()) {
    return QVariant();
  }

  const InspectorPacket& packet = m_history[index.row()];

  if (role == Qt::DisplayRole) {
    switch (index.column()) {
      case 0:
        return QString::fromStdString(packet.timestamp);
      case 1:
        return QString::fromStdString(packet.senderId);
      case 2:
        return QString::fromStdString(packet.key);
      case 3: {
        QString t = QString::fromStdString(packet.topic);
        return t.isEmpty() ? "[Empty]" : t;
      }
      case 4:
        return formatByteSize(packet.sizeBytes);
      case 5:
        return formatByteSize(packet.payload.size());
    }
  } else if (role == Qt::TextAlignmentRole && (index.column() == 4 || index.column() == 5)) {
    return int(Qt::AlignRight | Qt::AlignVCenter);
  } else if (role == Qt::BackgroundRole) {
    if (packet.key == Keys::CONNECT) {
      return QColor(0, 128, 0, 100);
    } else if (packet.key == Keys::DISCONNECT) {
      return QColor(128, 0, 0, 100);
    } else if (packet.key == Keys::SUBSCRIBE) {
      return QColor(0, 0, 128, 100);
    } else if (packet.key == Keys::UNSUBSCRIBE) {
      return QColor(0, 128, 128, 100);
    } else if (packet.key == Keys::SYS_STATS) {
      return QColor(128, 128, 0, 100);
    } else if (Keys::isSystemPacket(packet.key)) {
      return QColor(50, 50, 50, 100);
    } else {
      // Standard color
    }
  }
  return QVariant();
}

PacketFilterProxyModel::PacketFilterProxyModel(QObject* parent) : QSortFilterProxyModel(parent) {}

void PacketFilterProxyModel::updateFilters(const QString& text, const QSet<QString>& topics) {
  // invalidateFilter() is deprecated from Qt 6.9; the replacement pair does
  // not exist before it, so both spellings have to be kept while either Qt is
  // supported. The contract is begin -> mutate the filter state -> end.
#if QT_VERSION >= QT_VERSION_CHECK(6, 9, 0)
  beginFilterChange();
#endif
  m_searchText = text.toLower();
  m_allowedTopics = topics;
#if QT_VERSION >= QT_VERSION_CHECK(6, 9, 0)
  endFilterChange();
#else
  invalidateFilter();
#endif
}

bool PacketFilterProxyModel::filterAcceptsRow(int sourceRow, const QModelIndex& sourceParent) const {
  QModelIndex topicIdx = sourceModel()->index(sourceRow, 3, sourceParent);
  const QString topic = sourceModel()->data(topicIdx).toString();

  if (!m_allowedTopics.contains(topic)) {
    return false;
  }

  if (m_searchText.isEmpty()) {
    return true;
  }

  const QString sender = sourceModel()->data(sourceModel()->index(sourceRow, 1, sourceParent)).toString().toLower();
  const QString key = sourceModel()->data(sourceModel()->index(sourceRow, 2, sourceParent)).toString().toLower();

  return sender.contains(m_searchText) || key.contains(m_searchText) || topic.toLower().contains(m_searchText);
}