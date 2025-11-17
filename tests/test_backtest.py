"""
Tests for backtest components
"""

from datetime import date
from decimal import Decimal

import pytest

from app.backtest.engine import BacktestResult, Portfolio, Position
from app.backtest.signals import SignalEngine, StrategyBuilder
from app.backtest.strategy import create_strategy, get_strategy


class TestPosition:
    """Tests for Position class"""

    def test_position_creation(self):
        """Test creating a position"""
        position = Position(
            stock_code="600000",
            stock_name="浦发银行",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={"type": "test"},
        )

        assert position.stock_code == "600000"
        assert position.shares == 1000
        assert position.position_value == Decimal("10000.00")

    def test_calculate_pnl_profit(self):
        """Test P&L calculation with profit"""
        position = Position(
            stock_code="600000",
            stock_name="浦发银行",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        # 10% price increase
        pnl = position.calculate_pnl(Decimal("11.00"))

        assert pnl["gross_pnl"] == Decimal("1000.00")
        assert pnl["net_pnl"] > Decimal("0")  # Positive after costs
        assert pnl["return_pct"] > Decimal("0")

    def test_calculate_pnl_loss(self):
        """Test P&L calculation with loss"""
        position = Position(
            stock_code="600000",
            stock_name="浦发银行",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        # 5% price decrease
        pnl = position.calculate_pnl(Decimal("9.50"))

        assert pnl["gross_pnl"] == Decimal("-500.00")
        assert pnl["net_pnl"] < Decimal("0")  # Negative
        assert pnl["return_pct"] < Decimal("0")


class TestPortfolio:
    """Tests for Portfolio class"""

    def test_portfolio_initialization(self):
        """Test portfolio initialization"""
        portfolio = Portfolio(initial_capital=Decimal("1000000"))

        assert portfolio.cash == Decimal("1000000")
        assert portfolio.total_value == Decimal("1000000")
        assert len(portfolio.positions) == 0
        assert len(portfolio.closed_positions) == 0

    def test_open_position(self):
        """Test opening a position"""
        portfolio = Portfolio(initial_capital=Decimal("1000000"))

        portfolio.open_position(
            stock_code="600000",
            stock_name="浦发银行",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        assert len(portfolio.positions) == 1
        assert "600000" in portfolio.positions
        # Cash reduced by position value + costs
        assert portfolio.cash < Decimal("990000")

    def test_open_position_insufficient_cash(self):
        """Test opening position with insufficient cash"""
        portfolio = Portfolio(initial_capital=Decimal("1000"))

        with pytest.raises(ValueError, match="Insufficient cash"):
            portfolio.open_position(
                stock_code="600000",
                stock_name="浦发银行",
                entry_date=date(2025, 1, 1),
                entry_price=Decimal("10.00"),
                shares=1000,
                entry_signal={},
            )

    def test_close_position(self):
        """Test closing a position"""
        portfolio = Portfolio(initial_capital=Decimal("1000000"))

        # Open position
        portfolio.open_position(
            stock_code="600000",
            stock_name="浦发银行",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        initial_positions = len(portfolio.positions)

        # Close position with profit
        trade = portfolio.close_position(
            stock_code="600000",
            exit_date=date(2025, 1, 2),
            exit_price=Decimal("11.00"),
            exit_reason="take_profit",
        )

        assert len(portfolio.positions) == initial_positions - 1
        assert len(portfolio.closed_positions) == 1
        assert trade["net_pnl"] > Decimal("0")
        assert trade["exit_reason"] == "take_profit"

    def test_close_position_not_found(self):
        """Test closing a non-existent position"""
        portfolio = Portfolio(initial_capital=Decimal("1000000"))

        with pytest.raises(ValueError, match="No open position"):
            portfolio.close_position(
                stock_code="600000",
                exit_date=date(2025, 1, 2),
                exit_price=Decimal("11.00"),
                exit_reason="test",
            )

    def test_portfolio_snapshot(self):
        """Test taking portfolio snapshot"""
        portfolio = Portfolio(initial_capital=Decimal("1000000"))

        snapshot = portfolio.snapshot(date(2025, 1, 1))

        assert snapshot["cash"] == Decimal("1000000")
        assert snapshot["total_value"] == Decimal("1000000")
        assert snapshot["open_positions"] == 0
        assert len(portfolio.equity_curve) == 1


class TestBacktestResult:
    """Tests for BacktestResult class"""

    def test_empty_result(self):
        """Test result with no trades"""
        result = BacktestResult(
            trades=[],
            equity_curve=[{"snapshot_date": date(2025, 1, 1), "total_value": Decimal("1000000")}],
            initial_capital=Decimal("1000000"),
        )

        assert result.total_trades == 0
        assert result.win_rate == 0
        assert result.total_return == 0

    def test_result_with_trades(self):
        """Test result with winning trades"""
        trades = [
            {
                "net_pnl": Decimal("1000"),
                "gross_pnl": Decimal("1100"),
                "holding_days": 5,
            },
            {
                "net_pnl": Decimal("500"),
                "gross_pnl": Decimal("550"),
                "holding_days": 3,
            },
            {
                "net_pnl": Decimal("-300"),
                "gross_pnl": Decimal("-330"),
                "holding_days": 2,
            },
        ]

        equity_curve = [
            {"snapshot_date": date(2025, 1, 1), "total_value": Decimal("1000000"), "daily_return": 0},
            {"snapshot_date": date(2025, 1, 2), "total_value": Decimal("1001000"), "daily_return": 0.1},
        ]

        result = BacktestResult(
            trades=trades,
            equity_curve=equity_curve,
            initial_capital=Decimal("1000000"),
        )

        assert result.total_trades == 3
        assert result.winning_trades == 2
        assert result.losing_trades == 1
        assert result.win_rate == pytest.approx(66.67, rel=0.1)


class TestSignalEngine:
    """Tests for SignalEngine class"""

    def test_signal_engine_initialization(self):
        """Test initializing signal engine"""
        strategy_def = StrategyBuilder.tail_scraper_strategy()
        engine = SignalEngine(strategy_def)

        assert engine.take_profit_pct == Decimal("10.0")
        assert engine.stop_loss_pct == Decimal("-5.0")
        assert engine.max_holding_days == 30

    def test_check_exit_take_profit(self):
        """Test exit condition: take profit"""
        strategy_def = StrategyBuilder.tail_scraper_strategy(take_profit_pct=10.0)
        signal_engine = SignalEngine(strategy_def)

        position = Position(
            stock_code="600000",
            stock_name="Test",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        # 15% profit - should trigger
        exit_reason = signal_engine.check_exit(
            position=position,
            current_price=Decimal("11.50"),
            current_date=date(2025, 1, 2),
            engine=None,
        )

        assert exit_reason == "take_profit"

    def test_check_exit_stop_loss(self):
        """Test exit condition: stop loss"""
        strategy_def = StrategyBuilder.tail_scraper_strategy(stop_loss_pct=-5.0)
        signal_engine = SignalEngine(strategy_def)

        position = Position(
            stock_code="600000",
            stock_name="Test",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        # -6% loss - should trigger
        exit_reason = signal_engine.check_exit(
            position=position,
            current_price=Decimal("9.40"),
            current_date=date(2025, 1, 2),
            engine=None,
        )

        assert exit_reason == "stop_loss"

    def test_check_exit_time_limit(self):
        """Test exit condition: time limit"""
        strategy_def = StrategyBuilder.tail_scraper_strategy(max_holding_days=10)
        signal_engine = SignalEngine(strategy_def)

        position = Position(
            stock_code="600000",
            stock_name="Test",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        # 11 days later - should trigger
        exit_reason = signal_engine.check_exit(
            position=position,
            current_price=Decimal("10.50"),
            current_date=date(2025, 1, 12),
            engine=None,
        )

        assert exit_reason == "time_limit"

    def test_check_exit_no_trigger(self):
        """Test exit condition: no trigger"""
        strategy_def = StrategyBuilder.tail_scraper_strategy()
        signal_engine = SignalEngine(strategy_def)

        position = Position(
            stock_code="600000",
            stock_name="Test",
            entry_date=date(2025, 1, 1),
            entry_price=Decimal("10.00"),
            shares=1000,
            entry_signal={},
        )

        # Small profit, short holding - should not trigger
        exit_reason = signal_engine.check_exit(
            position=position,
            current_price=Decimal("10.20"),
            current_date=date(2025, 1, 2),
            engine=None,
        )

        assert exit_reason is None


class TestStrategyBuilder:
    """Tests for StrategyBuilder class"""

    def test_tail_scraper_strategy_defaults(self):
        """Test building tail scraper strategy with defaults"""
        strategy = StrategyBuilder.tail_scraper_strategy()

        assert strategy["name"] == "Tail Scraper"
        assert strategy["version"] == "1.0.0"
        assert strategy["take_profit_pct"] == 10.0
        assert strategy["stop_loss_pct"] == -5.0
        assert strategy["max_holding_days"] == 30

    def test_tail_scraper_strategy_custom(self):
        """Test building tail scraper strategy with custom parameters"""
        strategy = StrategyBuilder.tail_scraper_strategy(
            take_profit_pct=15.0,
            stop_loss_pct=-8.0,
            max_holding_days=45,
        )

        assert strategy["take_profit_pct"] == 15.0
        assert strategy["stop_loss_pct"] == -8.0
        assert strategy["max_holding_days"] == 45


class TestStrategyDatabase:
    """Tests for strategy database operations"""

    @pytest.mark.integration
    def test_create_strategy(self, db_session, in_memory_engine):
        """Test creating a strategy in database"""
        from app.backtest.strategy import create_strategy

        strategy_def = StrategyBuilder.tail_scraper_strategy()
        strategy_id = create_strategy(
            engine=in_memory_engine,
            name="Test Strategy",
            version="1.0.0",
            definition=strategy_def,
            description="Test description",
        )

        assert strategy_id is not None

    @pytest.mark.integration
    def test_get_strategy(self, in_memory_engine):
        """Test retrieving a strategy from database"""
        from app.backtest.strategy import create_strategy, get_strategy

        strategy_def = StrategyBuilder.tail_scraper_strategy()
        strategy_id = create_strategy(
            engine=in_memory_engine,
            name="Test Strategy 2",
            version="1.0.0",
            definition=strategy_def,
        )

        # Get by ID
        strategy = get_strategy(in_memory_engine, strategy_id=strategy_id)
        assert strategy is not None
        assert strategy.name == "Test Strategy 2"

        # Get by name and version
        strategy = get_strategy(in_memory_engine, name="Test Strategy 2", version="1.0.0")
        assert strategy is not None
        assert strategy.id == strategy_id
