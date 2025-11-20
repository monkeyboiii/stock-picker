import { View, Text, StyleSheet, FlatList, TouchableOpacity } from 'react-native';
import { createBacktestClient } from '@repo/api/backtest';
import { colors } from '@repo/ui';

const _backtestClient = createBacktestClient(
  process.env.EXPO_PUBLIC_BACKTEST_API_URL || 'http://localhost:8001'
);

// Placeholder data
const PLACEHOLDER_BACKTESTS = [
  {
    id: '1',
    name: 'MA Crossover Strategy',
    status: 'completed',
    totalReturn: 12.5,
    sharpeRatio: 1.8,
    maxDrawdown: -8.2,
    winRate: 68,
    createdAt: '2025-11-15',
  },
  {
    id: '2',
    name: 'RSI Mean Reversion',
    status: 'running',
    totalReturn: 0,
    sharpeRatio: 0,
    maxDrawdown: 0,
    winRate: 0,
    createdAt: '2025-11-18',
  },
  {
    id: '3',
    name: 'Bollinger Bands Breakout',
    status: 'completed',
    totalReturn: -3.4,
    sharpeRatio: 0.6,
    maxDrawdown: -12.5,
    winRate: 45,
    createdAt: '2025-11-10',
  },
];

export default function BacktestsScreen() {
  const renderBacktestItem = ({
    item,
  }: {
    item: typeof PLACEHOLDER_BACKTESTS[0];
  }) => {
    const isPositive = item.totalReturn >= 0;
    const statusColor =
      item.status === 'completed'
        ? colors.success
        : item.status === 'running'
          ? colors.info
          : colors.error;

    return (
      <TouchableOpacity style={styles.backtestCard}>
        <View style={styles.cardHeader}>
          <View style={styles.headerLeft}>
            <Text style={styles.backtestName}>{item.name}</Text>
            <Text style={styles.backtestDate}>{item.createdAt}</Text>
          </View>
          <View style={[styles.statusBadge, { backgroundColor: statusColor }]}>
            <Text style={styles.statusText}>
              {item.status.toUpperCase()}
            </Text>
          </View>
        </View>

        {item.status === 'completed' && (
          <View style={styles.metricsGrid}>
            <View style={styles.metric}>
              <Text style={styles.metricLabel}>Total Return</Text>
              <Text
                style={[
                  styles.metricValue,
                  { color: isPositive ? colors.bullish : colors.bearish },
                ]}
              >
                {isPositive ? '+' : ''}
                {item.totalReturn.toFixed(2)}%
              </Text>
            </View>

            <View style={styles.metric}>
              <Text style={styles.metricLabel}>Sharpe Ratio</Text>
              <Text style={styles.metricValue}>{item.sharpeRatio.toFixed(2)}</Text>
            </View>

            <View style={styles.metric}>
              <Text style={styles.metricLabel}>Max Drawdown</Text>
              <Text style={[styles.metricValue, { color: colors.bearish }]}>
                {item.maxDrawdown.toFixed(2)}%
              </Text>
            </View>

            <View style={styles.metric}>
              <Text style={styles.metricLabel}>Win Rate</Text>
              <Text style={styles.metricValue}>{item.winRate}%</Text>
            </View>
          </View>
        )}

        {item.status === 'running' && (
          <View style={styles.runningContainer}>
            <Text style={styles.runningText}>Backtest in progress...</Text>
          </View>
        )}
      </TouchableOpacity>
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.headerContainer}>
        <TouchableOpacity style={styles.newButton}>
          <Text style={styles.newButtonText}>+ New Backtest</Text>
        </TouchableOpacity>
      </View>

      <FlatList
        data={PLACEHOLDER_BACKTESTS}
        renderItem={renderBacktestItem}
        keyExtractor={(item) => item.id}
        contentContainerStyle={styles.listContent}
        ItemSeparatorComponent={() => <View style={styles.separator} />}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#f9fafb',
  },
  headerContainer: {
    padding: 16,
    backgroundColor: '#ffffff',
    borderBottomWidth: 1,
    borderBottomColor: '#e5e7eb',
  },
  newButton: {
    backgroundColor: colors.primary[600],
    paddingVertical: 12,
    paddingHorizontal: 24,
    borderRadius: 8,
    alignItems: 'center',
  },
  newButtonText: {
    color: '#ffffff',
    fontSize: 16,
    fontWeight: '600',
  },
  listContent: {
    padding: 16,
  },
  backtestCard: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 16,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.1,
    shadowRadius: 2,
    elevation: 2,
  },
  cardHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 16,
  },
  headerLeft: {
    flex: 1,
  },
  backtestName: {
    fontSize: 16,
    fontWeight: '600',
    color: '#111827',
    marginBottom: 4,
  },
  backtestDate: {
    fontSize: 12,
    color: '#9ca3af',
  },
  statusBadge: {
    paddingVertical: 4,
    paddingHorizontal: 12,
    borderRadius: 12,
  },
  statusText: {
    color: '#ffffff',
    fontSize: 10,
    fontWeight: '600',
  },
  metricsGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 12,
  },
  metric: {
    width: '48%',
    backgroundColor: '#f9fafb',
    padding: 12,
    borderRadius: 8,
  },
  metricLabel: {
    fontSize: 12,
    color: '#6b7280',
    marginBottom: 4,
  },
  metricValue: {
    fontSize: 18,
    fontWeight: '600',
    color: '#111827',
  },
  runningContainer: {
    paddingVertical: 24,
    alignItems: 'center',
  },
  runningText: {
    fontSize: 14,
    color: '#6b7280',
    fontStyle: 'italic',
  },
  separator: {
    height: 12,
  },
});
