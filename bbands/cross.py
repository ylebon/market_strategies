import asyncio
import os

from logbook import Logger

from events import Events
from indicators.core.candle.candle_element import CandleElement
from indicators.core.candle.candle_rolling import CandleRolling
from signaler.core import signaler
from events.signal import SellSignal, BuySignal


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

        # if isinstance(event, BuySignal) and event.instrument_str in self.success_buy:
        #     return False
        #
        # elif isinstance(event, SellSignal) and not event.instrument_str in self.success_buy:
        #     return False

        return True

class Signaler(signaler.Signaler):
    """
    Bollinger bands Signal based on mean

    Send BuySignal when price lower than low band and sell when high
    Execute after 12h on monday
    """

    def __init__(self, name, context):
        signaler.Signaler.__init__(self, name, context)
        self.log = Logger(str(self))
        self.slow_candle_rolling = dict()
        self.fast_candle_rolling = dict()
        self.buy_signal = dict()
        self.wallet = None
        self.candle_rolling_direction = dict()
        self.slow_rolling_window = None
        self.fast_rolling_window = None

    def initialize(self):
        """
        Initialize strategy

        """
        # read context  variable
        self.slow_rolling_window = int(os.getenv("SIGNALER__SLOW_WINDOW", 3600 * 4))
        self.fast_rolling_window = int(os.getenv("SIGNALER__FAST_WINDOW", 3600 * 1))
        self.wallet = float(os.getenv("SIGNALER__WALLET", 100))

        # log
        self.log.info(
            f"msg='create rolling time list' "
            f"slow_rolling_window='{self.slow_rolling_window}' "
            f"fast_rolling_window='{self.fast_rolling_window}'"
        )

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
            self.slow_candle_rolling[feed].update(
                candle_element
            )
            self.fast_candle_rolling[feed].update(
                candle_element
            )
        except KeyError:
            self.slow_candle_rolling[feed] = CandleRolling.from_empty(self.slow_rolling_window)
            self.fast_candle_rolling[feed] = CandleRolling.from_empty(self.fast_rolling_window)
            self.slow_candle_rolling[feed].update(
                candle_element
            )
            self.fast_candle_rolling[feed].update(
                candle_element
            )

        # return
        if not self.slow_candle_rolling[feed].full:
            return

        slow_mean = self.slow_candle_rolling[feed].container.get_mean()
        fast_mean = self.fast_candle_rolling[feed].container.get_mean()


        # BUY signal
        buy_signal = self.buy_signal.get(feed)
        if not buy_signal and slow_mean  < fast_mean:
            output_signal = self.create_signal(
                Events.SIGNAL_BUY,
                tick=input_event,
                qty=0,
                total_price=self.wallet,
                ordering_info={
                    'time_in_force': 'GTC'
                },
                normalize=True
            )

            self.buy_signal[feed] = output_signal
            await self.callback(output_signal)

        # SELL signal
        elif buy_signal and fast_mean < slow_mean:
            output_signal = self.create_signal(
                Events.SIGNAL_SELL,
                tick=input_event,
                qty=buy_signal.qty,
                buy_signal_id=buy_signal.id,
                ordering_info={
                    'time_in_force': 'GTC'
                },
                normalize=True
            )
            self.buy_signal[feed] = None
            await self.callback(output_signal)