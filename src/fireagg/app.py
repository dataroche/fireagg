from fastapi import FastAPI

from .database import db, symbols, symbol_prices

app = FastAPI()


@app.get("/true-mid-price/{symbol}")
async def read_true_mid_price(symbol: str):
    async with db.connect_async() as commands:
        symbol_obj = await symbols.get_symbol(commands, symbol.replace("-", "/"))
        if symbol_obj:
            last_true_price = await symbol_prices.get_last_symbol_true_mid_price(
                commands, symbol_obj.id
            )
            return last_true_price and last_true_price["true_mid_price"]
        else:
            return f"ERR: No symbol {symbol}"
