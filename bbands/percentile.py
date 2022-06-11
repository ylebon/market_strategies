from logbook import Logger
import asyncio
from events import Events
from events.signal import SellSignal, BuySignal
from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling
from signaler.core import signaler

class OrderingHandler(signaler.OrderingHandler):
    """
    Ordering handler

    """

    def __init__(self):
        signaler.OrderingHandler.__init__(self)
        self.failed_buy = dict()
        self.success_buy = dict()
        self.lock = asyncio.Lock()

    async def on_buy_limit_order_expired(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None
            self.failed_buy[event.signal_id] = event

    async def on_buy_limit_order_filled(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None
            self.success_buy[event.instrument_str] = event

    async def on_buy_limit_order_new(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None

    async def on_buy_limit_order_error(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None
            self.failed_buy[event.signal_id] = event

    async def on_sell_limit_order_expired(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None

    async def on_sell_limit_order_new(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None

    async def on_sell_limit_order_error(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None

    async def on_sell_limit_order_filled(self, event):
        async with self.lock:
            self.on_route[event.instrument_str] = None
            self.success_buy[event.instrument_str] = None

    async def set_on_route(self, event, request):
        async with self.lock:
            self.on_route[event.instrument_str] = request

    def can_order(self, event):
        """
        Can we order

        """
        if self.on_route.get(event.instrument_str, None):
            return False

        if isinstance(event, BuySignal) and event.instrument_str in self.success_buy:
            return False

        elif isinstance(event, SellSignal) and not event.instrument_str in self.success_buy:
            return False

        return True

class Signaler(signaler.Signaler):
    """
    Bollinger bands Signal

    Send BuySignal when price lower than low band and sell when high
    """

    def __init__(self, name, context):
        signaler.Signaler.__init__(self, name, context)
        self.log = Logger(str(self))
        self.candle_rolling = dict()
        self.candle_rolling_direction = dict()
        self.buy_signal = dict()
        self.percentile = None
        self.wallet = None
        self.rolling_window = None

    def initialize(self):
        """
        Initialize strategy

        """
        # read context  variable
        self.rolling_window = int(self.context.get("SIGNALER__ROLLING_WINDOW", 3600 * 4))
        self.wallet = float(self.context.get("SIGNALER__WALLET", 100))
        self.percentile = int(self.context.get("SIGNALER__PERCENTILE", 99))

        # log
        self.log.info(f"msg='create rolling time list' rolling_window={self.rolling_window}")

    async def on_bbo(self, input_event):
        """
        Return output signal

        """
        if not input_event.bid_price:
            return

        # candle
        new_state = (input_event.marketdata_date, input_event.bid_price)
        candle_element = CandleElement.from_tuple(*new_state)

        feed = input_event.instrument_str

        try:
            self.candle_rolling[feed].update(
                candle_element
            )
            self.candle_rolling_direction[feed].update(
                candle_element
            )
        except KeyError:
            self.candle_rolling[feed] = CandleRolling.from_empty(self.rolling_window)
            self.candle_rolling_direction[feed] = CandleRolling.from_empty(60 * 5)


        # return if warm up phase
        if not self.candle_rolling[feed].full:
           return

        # update indicators
        rolling_high = self.candle_rolling[feed].container.get_percentile(self.percentile)
        mean, upper_band, lower_band = self.candle_rolling[feed].container.get_bollinger(2)

        # update candle rolling direction
        c_open = self.candle_rolling_direction[feed].container.get_open()
        c_close = self.candle_rolling_direction[feed].container.get_close()

        # process signal
        buy_signal = self.buy_signal.get(feed)
        if not buy_signal and c_open < lower_band < c_close:
            output_signal = self.create_signal(
                Events.SIGNAL_BUY,
                tick=input_event,
                qty=0,
                total_price=self.wallet,
                ordering_info={
                    'time_in_force': 'GTC'
                },
                normalize=True,
                message=f''

            )
            self.buy_signa[feed] = output_signal
            await self.callback(output_signal)

        elif buy_signal and c_close < rolling_high < c_open:
            output_signal = self.create_signal(
                Events.SIGNAL_SELL,
                tick=input_event,
                qty=buy_signal.qty,
                buy_signal_id=buy_signal.id,
                ordering_info={
                    'check_position': False,
                    'time_in_force': 'GTC'
                },
                normalize=True
            )
            self.buy_signal[feed] = None
            await self.callback(output_signal)