from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.routing import Route
import uvicorn
import pandas as pd



async def price_data(request: Request) -> JSONResponse:
    """
    Return price data for the requested symbol
    """
    # read csv into pandas dataframe
    df = pd.read_csv('data/nifty50_all.csv', usecols=['Open', 'High', 'Low', 'Close', 'Symbol', 'Date'])

    # check validity of symbol
    symbol = request.path_params['symbol']
    if symbol not in df.Symbol.unique(): return JSONResponse({ "message": "Invalid symbol entered" }, status_code=400)

    # check validity of year
    filter_year = request.query_params.get('year')
    if (filter_year is not None) and (not filter_year.isdigit() or len(filter_year) != 4): return JSONResponse({ "message": "Invalid year entered" }, status_code=400)
    
    # if both valid, parse dataframe for relevant records
    symbol_specific_trades = df[df.Symbol == symbol].fillna('')
    symbol_specific_trades.Date = pd.to_datetime(symbol_specific_trades.Date)
    if filter_year: symbol_specific_trades = symbol_specific_trades[symbol_specific_trades.Date == filter_year]
    symbol_specific_trades.sort_values(by='Date', ascending=False)
    symbol_specific_trades.Date = symbol_specific_trades.Date.astype(str)
    symbol_specific_trades.rename(columns = {'Open':'open', 'Low':'low', 'High': 'high', 'Close': 'close'}, inplace = True)
    return JSONResponse(symbol_specific_trades.drop(columns=['Symbol', 'Date']).to_dict('records'))


    # TODO:
    # 1) Return open, high, low & close prices for the requested symbol as json records
    # 2) Allow calling app to filter the data by year using an optional query parameter

    # Symbol data is stored in the file data/nifty50_all.csv

# URL routes
app = Starlette(debug=True, routes=[
    Route('/nifty/stocks/{symbol}', price_data)
])


def main() -> None:
    """
    start the server
    """
    uvicorn.run(app, host='0.0.0.0', port=8888)


# Entry point
main()
