import { View, Text, StyleSheet, ScrollView, TouchableOpacity } from 'react-native';
import { useRouter } from 'expo-router';
import { colors } from '@repo/ui';

export default function DashboardOverviewScreen() {
  const router = useRouter();

  return (
    <ScrollView style={styles.container}>
      <View style={styles.statsGrid}>
        <View style={styles.statCard}>
          <Text style={styles.statIcon}>💹</Text>
          <Text style={styles.statLabel}>Total Stocks</Text>
          <Text style={styles.statValue}>5,247</Text>
        </View>

        <View style={styles.statCard}>
          <Text style={styles.statIcon}>🔬</Text>
          <Text style={styles.statLabel}>Active Backtests</Text>
          <Text style={styles.statValue}>12</Text>
        </View>

        <View style={styles.statCard}>
          <Text style={styles.statIcon}>📈</Text>
          <Text style={styles.statLabel}>Best Sharpe</Text>
          <Text style={styles.statValue}>2.34</Text>
        </View>

        <View style={styles.statCard}>
          <Text style={styles.statIcon}>✅</Text>
          <Text style={styles.statLabel}>Win Rate</Text>
          <Text style={styles.statValue}>68%</Text>
        </View>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Recent Activity</Text>
        <View style={styles.activityList}>
          <View style={styles.activityItem}>
            <View style={styles.activityInfo}>
              <Text style={styles.activityTitle}>
                Backtest completed: MA Crossover Strategy
              </Text>
              <Text style={styles.activityTime}>2 hours ago</Text>
            </View>
            <Text style={[styles.activityValue, { color: colors.success }]}>
              +12.5%
            </Text>
          </View>

          <View style={styles.activityItem}>
            <View style={styles.activityInfo}>
              <Text style={styles.activityTitle}>
                Stock filter updated: High Volume Stocks
              </Text>
              <Text style={styles.activityTime}>5 hours ago</Text>
            </View>
            <Text style={[styles.activityValue, { color: colors.info }]}>
              142 stocks
            </Text>
          </View>

          <View style={styles.activityItem}>
            <View style={styles.activityInfo}>
              <Text style={styles.activityTitle}>
                Portfolio optimization completed
              </Text>
              <Text style={styles.activityTime}>1 day ago</Text>
            </View>
            <Text style={[styles.activityValue, { color: colors.success }]}>
              Sharpe: 2.1
            </Text>
          </View>
        </View>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Quick Actions</Text>
        <View style={styles.actionGrid}>
          <TouchableOpacity
            style={styles.actionCard}
            onPress={() => router.push('/(dashboard)/stocks')}
          >
            <Text style={styles.actionIcon}>💹</Text>
            <Text style={styles.actionTitle}>Filter Stocks</Text>
            <Text style={styles.actionDescription}>
              Apply technical filters to find trading opportunities
            </Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={styles.actionCard}
            onPress={() => router.push('/(dashboard)/backtests')}
          >
            <Text style={styles.actionIcon}>🔬</Text>
            <Text style={styles.actionTitle}>Run Backtest</Text>
            <Text style={styles.actionDescription}>
              Test your strategy with historical data
            </Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={styles.actionCard}
            onPress={() => router.push('/(dashboard)/analytics')}
          >
            <Text style={styles.actionIcon}>📈</Text>
            <Text style={styles.actionTitle}>Calculate Metrics</Text>
            <Text style={styles.actionDescription}>
              Compute technical indicators and risk metrics
            </Text>
          </TouchableOpacity>
        </View>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#f9fafb',
  },
  statsGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    padding: 16,
    gap: 12,
  },
  statCard: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 16,
    width: '48%',
    alignItems: 'center',
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.1,
    shadowRadius: 2,
    elevation: 2,
  },
  statIcon: {
    fontSize: 32,
    marginBottom: 8,
  },
  statLabel: {
    fontSize: 12,
    color: '#6b7280',
    marginBottom: 4,
    textAlign: 'center',
  },
  statValue: {
    fontSize: 24,
    fontWeight: 'bold',
    color: '#111827',
  },
  section: {
    padding: 16,
  },
  sectionTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: '#111827',
    marginBottom: 16,
  },
  activityList: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 16,
    gap: 16,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.1,
    shadowRadius: 2,
    elevation: 2,
  },
  activityItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  activityInfo: {
    flex: 1,
  },
  activityTitle: {
    fontSize: 14,
    fontWeight: '500',
    color: '#111827',
    marginBottom: 4,
  },
  activityTime: {
    fontSize: 12,
    color: '#6b7280',
  },
  activityValue: {
    fontSize: 14,
    fontWeight: '600',
  },
  actionGrid: {
    gap: 12,
  },
  actionCard: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 16,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.1,
    shadowRadius: 2,
    elevation: 2,
  },
  actionIcon: {
    fontSize: 32,
    marginBottom: 8,
  },
  actionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: '#111827',
    marginBottom: 4,
  },
  actionDescription: {
    fontSize: 14,
    color: '#6b7280',
    lineHeight: 20,
  },
});
