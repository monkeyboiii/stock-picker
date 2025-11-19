import { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
  TextInput,
  Alert,
} from 'react-native';
import { createCalculationClient } from '@repo/api/calculation';
import { colors } from '@repo/ui';

const calculationClient = createCalculationClient(
  process.env.EXPO_PUBLIC_CALCULATION_API_URL || 'http://localhost:8005'
);

export default function AnalyticsScreen() {
  const [selectedTab, setSelectedTab] = useState<'indicators' | 'risk'>('indicators');

  const handleCalculate = (type: string) => {
    // TODO: Implement with calculationClient
    Alert.alert(
      'Coming Soon',
      `${type} calculation will be implemented with the Calculation API`
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.tabContainer}>
        <TouchableOpacity
          style={[styles.tab, selectedTab === 'indicators' && styles.tabActive]}
          onPress={() => setSelectedTab('indicators')}
        >
          <Text
            style={[
              styles.tabText,
              selectedTab === 'indicators' && styles.tabTextActive,
            ]}
          >
            Technical Indicators
          </Text>
        </TouchableOpacity>

        <TouchableOpacity
          style={[styles.tab, selectedTab === 'risk' && styles.tabActive]}
          onPress={() => setSelectedTab('risk')}
        >
          <Text
            style={[
              styles.tabText,
              selectedTab === 'risk' && styles.tabTextActive,
            ]}
          >
            Risk Analytics
          </Text>
        </TouchableOpacity>
      </View>

      <ScrollView style={styles.content}>
        {selectedTab === 'indicators' && (
          <View>
            <Text style={styles.sectionTitle}>Moving Averages</Text>
            <View style={styles.cardGrid}>
              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('SMA')}
              >
                <Text style={styles.cardTitle}>SMA</Text>
                <Text style={styles.cardDescription}>
                  Simple Moving Average
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('EMA')}
              >
                <Text style={styles.cardTitle}>EMA</Text>
                <Text style={styles.cardDescription}>
                  Exponential Moving Average
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('WMA')}
              >
                <Text style={styles.cardTitle}>WMA</Text>
                <Text style={styles.cardDescription}>
                  Weighted Moving Average
                </Text>
              </TouchableOpacity>
            </View>

            <Text style={styles.sectionTitle}>Momentum</Text>
            <View style={styles.cardGrid}>
              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('RSI')}
              >
                <Text style={styles.cardTitle}>RSI</Text>
                <Text style={styles.cardDescription}>
                  Relative Strength Index
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('MACD')}
              >
                <Text style={styles.cardTitle}>MACD</Text>
                <Text style={styles.cardDescription}>Moving Average Convergence Divergence</Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Stochastic')}
              >
                <Text style={styles.cardTitle}>Stochastic</Text>
                <Text style={styles.cardDescription}>
                  Stochastic Oscillator
                </Text>
              </TouchableOpacity>
            </View>

            <Text style={styles.sectionTitle}>Volatility</Text>
            <View style={styles.cardGrid}>
              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Bollinger Bands')}
              >
                <Text style={styles.cardTitle}>Bollinger Bands</Text>
                <Text style={styles.cardDescription}>
                  Price volatility bands
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('ATR')}
              >
                <Text style={styles.cardTitle}>ATR</Text>
                <Text style={styles.cardDescription}>
                  Average True Range
                </Text>
              </TouchableOpacity>
            </View>
          </View>
        )}

        {selectedTab === 'risk' && (
          <View>
            <Text style={styles.sectionTitle}>Risk-Adjusted Returns</Text>
            <View style={styles.cardGrid}>
              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Sharpe Ratio')}
              >
                <Text style={styles.cardTitle}>Sharpe Ratio</Text>
                <Text style={styles.cardDescription}>
                  Risk-adjusted return metric
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Sortino Ratio')}
              >
                <Text style={styles.cardTitle}>Sortino Ratio</Text>
                <Text style={styles.cardDescription}>
                  Downside risk metric
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Calmar Ratio')}
              >
                <Text style={styles.cardTitle}>Calmar Ratio</Text>
                <Text style={styles.cardDescription}>
                  Return/Drawdown ratio
                </Text>
              </TouchableOpacity>
            </View>

            <Text style={styles.sectionTitle}>Risk Metrics</Text>
            <View style={styles.cardGrid}>
              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('VaR')}
              >
                <Text style={styles.cardTitle}>VaR</Text>
                <Text style={styles.cardDescription}>
                  Value at Risk
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('CVaR')}
              >
                <Text style={styles.cardTitle}>CVaR</Text>
                <Text style={styles.cardDescription}>
                  Conditional VaR
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Max Drawdown')}
              >
                <Text style={styles.cardTitle}>Max Drawdown</Text>
                <Text style={styles.cardDescription}>
                  Maximum peak-to-trough decline
                </Text>
              </TouchableOpacity>
            </View>

            <Text style={styles.sectionTitle}>Portfolio Optimization</Text>
            <View style={styles.cardGrid}>
              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Mean-Variance')}
              >
                <Text style={styles.cardTitle}>Mean-Variance</Text>
                <Text style={styles.cardDescription}>
                  Portfolio optimization
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Efficient Frontier')}
              >
                <Text style={styles.cardTitle}>Efficient Frontier</Text>
                <Text style={styles.cardDescription}>
                  Optimal portfolio curve
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={styles.calculationCard}
                onPress={() => handleCalculate('Monte Carlo')}
              >
                <Text style={styles.cardTitle}>Monte Carlo</Text>
                <Text style={styles.cardDescription}>
                  Simulation analysis
                </Text>
              </TouchableOpacity>
            </View>
          </View>
        )}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#f9fafb',
  },
  tabContainer: {
    flexDirection: 'row',
    backgroundColor: '#ffffff',
    borderBottomWidth: 1,
    borderBottomColor: '#e5e7eb',
  },
  tab: {
    flex: 1,
    paddingVertical: 16,
    alignItems: 'center',
    borderBottomWidth: 2,
    borderBottomColor: 'transparent',
  },
  tabActive: {
    borderBottomColor: colors.primary[600],
  },
  tabText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#6b7280',
  },
  tabTextActive: {
    color: colors.primary[600],
  },
  content: {
    flex: 1,
    padding: 16,
  },
  sectionTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: '#111827',
    marginTop: 8,
    marginBottom: 16,
  },
  cardGrid: {
    gap: 12,
    marginBottom: 24,
  },
  calculationCard: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 16,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.1,
    shadowRadius: 2,
    elevation: 2,
  },
  cardTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: '#111827',
    marginBottom: 4,
  },
  cardDescription: {
    fontSize: 14,
    color: '#6b7280',
  },
});
