"""
Docstring for DataSupply.fundamental_data_management
skip for now.
"""

# def get_stock_detail(self, ticker: str):
#     """get local data firts + return a async future"""
#     provider = FloatSharesProvider()

#     local_float = provider.fetch_from_local(ticker)
#     logger.debug(f" testing {ticker}: {local_float}")

#     float_shares = None
#     if local_float:
#         float_shares = local_float.data[0].float_shares
#         logger.debug(f"Found local float data for {ticker}: {float_shares}")

#     # create a async web fetcher
#     future = self.executor.submit(self._fetch_extra_float_sync, ticker)

#     return float_shares, future

# def _fetch_extra_float_sync(self, ticker: str) -> Optional[Dict]:
#     """thread pool wrap func"""
#     try:
#         # create new task in this thread pool
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         try:
#             result = loop.run_until_complete(self._fetch_extra_float_async(ticker))
#             return result
#         finally:
#             loop.close()
#     except Exception as e:
#         logger.error(f"Error fetching extra float data for {ticker}: {e}")
#         return None

# async def _fetch_extra_float_async(self, ticker: str) -> Optional[Dict]:
#     """fecth web float data"""
#     try:
#         web_data = await FloatSharesProvider.fetch_from_web(ticker)
#         if not web_data or not web_data.data:
#             logger.warning(f"{ticker} no web float data found.")
#             return None

#         extra = {
#             "float_sources": [
#                 {
#                     d.source: {
#                         "float": d.float_shares,
#                         "short%": d.short_percent,
#                         "outstanding": d.outstanding_shares,
#                     }
#                 }
#                 for d in web_data.data
#             ]
#         }
#         logger.debug(f"Debug extra:\n{ticker}\n{extra}")

#         return extra
#     except Exception as e:
#         logger.error(f"Error in _fetch_extra_float_async for {ticker}: {e}")
#         return None

# def _handle_async_float_result(self, ticker, future):
#     result = future.result()
#     if not result:
#         return

#     self.socketio.emit(
#         "stock_float_extra_response", {"ticker": ticker, "extra": result}
#     )

# def _serialize_datetime_objects(self, obj):
# """Recursively convert datetime objects to ISO format strings"""
# if isinstance(obj, datetime):
#     return obj.isoformat()
# elif isinstance(obj, dict):
#     return {
#         key: self._serialize_datetime_objects(value)
#         for key, value in obj.items()
#     }
# elif isinstance(obj, list):
#     return [self._serialize_datetime_objects(item) for item in obj]
# else:
#     return obj
