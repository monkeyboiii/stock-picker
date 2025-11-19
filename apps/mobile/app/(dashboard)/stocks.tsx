import { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  TouchableOpacity,
  TextInput,
} from 'react-native';
import { createTradingClient } from '@repo/api/trading';
import { colors } from '@repo/ui';

const _tradingClient = createTradingClient(
  process.env.EXPO_PUBLIC_TRADING_API_URL || 'http://localhost:8000'
);

// Placeholder data
const PLACEHOLDER_STOCKS = [
  {
    code: '600000',
    name: 'Pudong Development Bank',
    price: 8.45,
    change: 0.12,
    changePercent: 1.44,
    volume: 1234567,
  },
  {
    code: '600519',
    name: 'Kweichow Moutai',
    price: 1678.5,
    change: -12.3,
    changePercent: -0.73,
    volume: 987654,
  },
  {
    code: '000001',
    name: 'Ping An Bank',
    price: 12.34,
    change: 0.45,
    changePercent: 3.78,
    volume: 2345678,
  },
  {
    code: '000002',
    name: 'Vanke A',
    price: 18.67,
    change: -0.23,
    changePercent: -1.22,
    volume: 1567890,
  },
];

export default function StocksScreen() {
  const [searchQuery, setSearchQuery] = useState('');

  const renderStockItem = ({ item }: { item: typeof PLACEHOLDER_STOCKS[0] }) => {
    const isPositive = item.changePercent >= 0;

    return (
      <TouchableOpacity style={styles.stockCard}>
        <View style={styles.stockHeader}>
          <View style={styles.stockInfo}>
            <Text style={styles.stockCode}>{item.code}</Text>
            <Text style={styles.stockName}>{item.name}</Text>
          </View>
          <View style={styles.stockPriceInfo}>
            <Text style={styles.stockPrice}>¥{item.price.toFixed(2)}</Text>
            <Text
              style={[
                styles.stockChange,
                { color: isPositive ? colors.bullish : colors.bearish },
              ]}
            >
              {isPositive ? '+' : ''}
              {item.changePercent.toFixed(2)}%
            </Text>
          </View>
        </View>
        <View style={styles.stockFooter}>
          <Text style={styles.stockVolume}>
            Vol: {(item.volume / 10000).toFixed(1)}万
          </Text>
        </View>
      </TouchableOpacity>
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.searchContainer}>
        <TextInput
          style={styles.searchInput}
          placeholder="Search stocks..."
          value={searchQuery}
          onChangeText={setSearchQuery}
          placeholderTextColor="#9ca3af"
        />
      </View>

      <View style={styles.filtersContainer}>
        <TouchableOpacity style={styles.filterChip}>
          <Text style={styles.filterChipText}>All</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.filterChipOutline}>
          <Text style={styles.filterChipOutlineText}>Shanghai</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.filterChipOutline}>
          <Text style={styles.filterChipOutlineText}>Shenzhen</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.filterChipOutline}>
          <Text style={styles.filterChipOutlineText}>High Volume</Text>
        </TouchableOpacity>
      </View>

      <FlatList
        data={PLACEHOLDER_STOCKS}
        renderItem={renderStockItem}
        keyExtractor={(item) => item.code}
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
  searchContainer: {
    padding: 16,
    backgroundColor: '#ffffff',
    borderBottomWidth: 1,
    borderBottomColor: '#e5e7eb',
  },
  searchInput: {
    backgroundColor: '#f9fafb',
    borderRadius: 8,
    paddingVertical: 10,
    paddingHorizontal: 16,
    fontSize: 16,
    color: '#111827',
  },
  filtersContainer: {
    flexDirection: 'row',
    padding: 16,
    gap: 8,
    backgroundColor: '#ffffff',
    borderBottomWidth: 1,
    borderBottomColor: '#e5e7eb',
  },
  filterChip: {
    backgroundColor: colors.primary[600],
    paddingVertical: 8,
    paddingHorizontal: 16,
    borderRadius: 16,
  },
  filterChipText: {
    color: '#ffffff',
    fontSize: 14,
    fontWeight: '600',
  },
  filterChipOutline: {
    backgroundColor: 'transparent',
    borderWidth: 1,
    borderColor: colors.primary[600],
    paddingVertical: 8,
    paddingHorizontal: 16,
    borderRadius: 16,
  },
  filterChipOutlineText: {
    color: colors.primary[600],
    fontSize: 14,
    fontWeight: '600',
  },
  listContent: {
    padding: 16,
  },
  stockCard: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 16,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.1,
    shadowRadius: 2,
    elevation: 2,
  },
  stockHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 8,
  },
  stockInfo: {
    flex: 1,
  },
  stockCode: {
    fontSize: 16,
    fontWeight: '600',
    color: '#111827',
    marginBottom: 4,
  },
  stockName: {
    fontSize: 14,
    color: '#6b7280',
  },
  stockPriceInfo: {
    alignItems: 'flex-end',
  },
  stockPrice: {
    fontSize: 18,
    fontWeight: 'bold',
    color: '#111827',
    marginBottom: 4,
  },
  stockChange: {
    fontSize: 14,
    fontWeight: '600',
  },
  stockFooter: {
    flexDirection: 'row',
    justifyContent: 'space-between',
  },
  stockVolume: {
    fontSize: 12,
    color: '#9ca3af',
  },
  separator: {
    height: 12,
  },
});
