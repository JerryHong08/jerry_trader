
# IBBot Roadmap

## 功能

1. 手动下单
2. 撤单
3. 持仓管理
4. 历史订单查询
5. 订单状态实时更新（websocket）
6. 自动交易策略扩展

Structure:

``` bash
src/
  IBBot/
    __init__.py
    main.py

    adapter/
      __init__.py
      ib_gateway.py
      ibkr_client.py
      ibkr_wrapper.py
      event_bus.py

    services/
      __init__.py
      order_service.py
      portfolio_service.py

    models/
      __init__.py
      order.py
      contract.py
      order_models.py
      portfolio_models.py

    api/
      __init__.py
      routes_orders.py
      routes_portfolio.py
      server.py
```
