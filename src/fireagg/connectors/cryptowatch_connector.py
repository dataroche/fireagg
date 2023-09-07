import time
import cryptowatch as cw

cw.api_key = "YI1WY79E8NOB7UJHILRZ"
cw.stream.subscriptions = ["markets:*:trades"]


def handle_trades_update(trade_update):
    """
    trade_update follows Cryptowatch protocol buffer format:
    https://github.com/cryptowatch/proto/blob/master/public/markets/market.proto
    """
    now = time.time()
    # print()
    # market_msg = ">>> Market#{} Exchange#{} Pair#{}: {} New Trades".format(
    #     trade_update.marketUpdate.market.marketId,
    #     trade_update.marketUpdate.market.exchangeId,
    #     trade_update.marketUpdate.market.currencyPairId,
    #     len(trade_update.marketUpdate.tradesUpdate.trades),
    # )
    # print(market_msg)
    for trade in trade_update.marketUpdate.tradesUpdate.trades:
        print(f"Latency = {(now - trade.timestamp):.2f}s")
        # trade_msg = "\tID:{} TIMESTAMP:{} TIMESTAMPNANO:{} PRICE:{} AMOUNT:{}".format(
        #     trade.externalId,
        #     trade.timestamp,
        #     trade.timestampNano,
        #     trade.priceStr,
        #     trade.amountStr,
        # )
        # print(trade_msg)


cw.stream.on_trades_update = handle_trades_update


# Start receiving
cw.stream.connect()
