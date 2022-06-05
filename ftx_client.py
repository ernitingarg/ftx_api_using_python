import time
import urllib.parse
from typing import Optional, Dict, Any, List
from requests import Request, Session, Response
import hmac


class FtxClient:

    def __init__(
        self, 
        api_endpoint: str, 
        api_key: str, 
        api_secret: str, 
        subaccount_name: str=None) -> None:
        """Constructor

        Args:
            api_endpoint (str): endpoint for api
            api_key (str): key of api
            api_secret (str): secret of api
            subaccount_name (str, optional): sub account name. Defaults to None.
        """
        self._validate_api_credentials(api_key, api_secret)
        self._session = Session()
        self._api_endpoint = api_endpoint
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name

    def get_all_futures(self) -> List[dict]:
        """Get all enabled and non-expired futures

        Returns:
            List[dict]: List of all enabled and non-expired futures
        """
        futures = self._get('futures')
        return [future for future in futures if 
                future['type'] == 'future' and 
                future['enabled'] == True and
                future['expired'] == False]

    def get_all_underlying_futures(self, underlying = 'BTC') -> List[dict]:
        """Get all enabled and non-expired futures for an underlying asset

        Args:
            underlying (str, optional): asset/token. Defaults to 'BTC'.

        Returns:
            List[dict]: List of all enabled and non-expired futures
        """
        all_futures = self.get_all_futures()
        return [future for future in all_futures if 
                future['underlying'] == underlying]
 
    def get_next_underlying_future(self, underlying = 'BTC') -> dict:
        """Get next enabled and non-expired future for an underlying asset

        Args:
            underlying (str, optional): asset/token. Defaults to 'BTC'.

        Returns:
            dict: Returns the next enabled and non-expired future
        """
        all_underlying_futures = self.get_all_underlying_futures(underlying)
        return min(all_underlying_futures, key=lambda future:future['expiry'])
    
    def get_next_underlying_future_name(self, underlying = 'BTC') -> str:
        """Get name of next enabled and non-expired future for an underlying asset

        Args:
            underlying (str, optional): asset/token. Defaults to 'BTC'.

        Returns:
            str: Returns the name of next enabled and non-expired future
        """
        all_underlying_futures = self.get_all_underlying_futures(underlying)
        return min(all_underlying_futures, key=lambda future:future['expiry'])['name']
    
    def get_all_markets(self) -> List[dict]:
        """Get all markets (eg: spot, perpetual futures, expiring futures, and MOVE contracts)

        Returns:
            List[dict]: List of all markets with their market parameters
        """
        return self._get('markets')
    
    def get_single_market(self, market: str) -> dict:
        """Get single market for a specific tarket market.

        Args:
            market (str): Target market (eg: BTC-PERP)

        Returns:
            dict: All market parameters of a market
        """
        return self._get(f'markets/{market}')
    
    def get_single_market_price(self, market: str) -> float:
        """Get current price of a specific tarket market.

        Args:
            market (str): Target market (eg: BTC-PERP)

        Returns:
            float: Target market price
        """
        return self.get_single_market(market)['price']
       
    def place_order(
        self, 
        market: str, 
        side: str, 
        size: float, 
        price: float=None, 
        type: str='market',
        reduce_only: bool=False, 
        ioc: bool=False, 
        post_only: bool=False, 
        client_id: str=None) -> dict:
        """Place an order

        Args:
            market (str): name of market (eg:BTC-PERP)
            side (str): 'buy' or 'sell'
            size (float): size of order
            price (float, optional): price, in case of `market` order use 'None'. Defaults to None.
            type (str, optional): Order type (limit, market etc). Defaults to 'market'.
            reduce_only (bool, optional): True means your position can only reduce in size if this order is triggered. Defaults to False.
            ioc (bool, optional): [description]. Defaults to False.
            post_only (bool, optional): [description]. Defaults to False.
            client_id (str, optional): [description]. Defaults to None.

        Returns:
            dict: returns the successfully placed order with dictonary of fields
        """
        return self._post('orders', {'market': market,
                                     'side': side,
                                     'price': price,
                                     'size': size,
                                     'type': type,
                                     'reduceOnly': reduce_only,
                                     'ioc': ioc,
                                     'postOnly': post_only,
                                     'clientId': client_id,
                                     })
        
    def get_open_orders(self, market: str=None) -> List[dict]:
        """Get all open orders for a given market

        Args:
            market (str, optional): name of market (eg:BTC-PERP). Defaults to None.

        Returns:
            List[dict]: List of all open orders
        """
        return self._get(f'orders', {'market': market})
    
    def get_order_history(
        self, 
        market: str=None, 
        side: str=None, 
        order_type: str ='market', 
        start_time: float=None, 
        end_time: float=None) -> List[dict]:
        """Get history of all orders

        Args:
            market (str, optional): name of market (eg:BTC-PERP). Defaults to None.
            side (str, optional): 'buy' or 'sell'. Defaults to None.
            order_type (str, optional): order type (limit, market etc). Defaults to 'market'.
            start_time (float, optional): start time. Defaults to None.
            end_time (float, optional): end time. Defaults to None.

        Returns:
            List[dict]: List of all open or closed orders
        """
        return self._get(f'orders/history', {'market': market, 'side': side, 'orderType': order_type, 'start_time': start_time, 'end_time': end_time})
    
    def _get(self, path: str, params: Optional[Dict[str, Any]]=None) -> Any:
        return self._request('GET', path, params=params)
    
    def _post(self, path: str, params: Optional[Dict[str, Any]]=None) -> Any:
        return self._request('POST', path, json=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._api_endpoint + path, **kwargs)
        self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self._subaccount_name)

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    def _validate_api_credentials(self, key, secret):
        if not key:
            raise ValueError('API key cannot be empty.')

        if not isinstance(key, str):
            raise ValueError('API key must be in a valid string format.')

        if not secret:
            raise ValueError('API secret cannot be empty.')

        if not isinstance(secret, str):
            raise ValueError('API secret must be in a valid string format.')