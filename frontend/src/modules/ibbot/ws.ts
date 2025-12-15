import type {
  AccountUpdatedEventData,
  OrderStatusEventData,
  PositionUpdatedEventData,
  WsMessage,
} from '../../types/ibbot';

export type IbbotWsHandlers = {
  onConnection?: (status: 'connected' | 'disconnected' | 'error', info?: string) => void;
  onOrderStatus?: (data: OrderStatusEventData) => void;
  onPositionUpdated?: (data: PositionUpdatedEventData) => void;
  onAccountUpdated?: (data: AccountUpdatedEventData) => void;
  onRawMessage?: (msg: WsMessage) => void;
};

export type IbbotWsClient = {
  close: () => void;
};

export function connectIbbotWs(wsUrl: string, handlers: IbbotWsHandlers): IbbotWsClient {
  let ws: WebSocket | null = null;
  let pingTimer: number | undefined;
  let reconnectTimer: number | undefined;
  let closedByUser = false;
  let backoffMs = 500;

  function clearTimers() {
    if (pingTimer) window.clearInterval(pingTimer);
    pingTimer = undefined;
    if (reconnectTimer) window.clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
  }

  function scheduleReconnect(reason?: string) {
    if (closedByUser) return;
    clearTimers();
    const wait = backoffMs;
    backoffMs = Math.min(backoffMs * 2, 10_000);
    handlers.onConnection?.('disconnected', reason ? `Reconnecting in ${Math.round(wait / 100) / 10}s: ${reason}` : `Reconnecting in ${Math.round(wait / 100) / 10}s`);
    reconnectTimer = window.setTimeout(() => {
      connect();
    }, wait);
  }

  function connect() {
    if (closedByUser) return;
    try {
      ws = new WebSocket(wsUrl);
    } catch (e) {
      scheduleReconnect(e instanceof Error ? e.message : String(e));
      return;
    }

    ws.onopen = () => {
      backoffMs = 500;
      handlers.onConnection?.('connected');
      // Keepalive: backend reads receive_text(), so send periodic pings.
      pingTimer = window.setInterval(() => {
        try {
          ws?.send('ping');
        } catch {
          // ignore
        }
      }, 20_000);
    };

    ws.onclose = () => {
      clearTimers();
      scheduleReconnect('socket closed');
    };

    ws.onerror = () => {
      handlers.onConnection?.('error', 'WebSocket error');
      // Let onclose drive reconnection if it happens; if not, a send failure
      // on the backend will close the socket and trigger onclose.
    };

    ws.onmessage = (evt) => {
      let msg: WsMessage;
      try {
        msg = JSON.parse(evt.data) as WsMessage;
      } catch {
        return;
      }

      handlers.onRawMessage?.(msg);

      if (msg.type === 'connection') {
        return;
      }

      switch (msg.event_name) {
        case 'OrderStatusEvent':
          handlers.onOrderStatus?.(msg.data as OrderStatusEventData);
          break;
        case 'PositionUpdatedEvent':
          handlers.onPositionUpdated?.(msg.data as PositionUpdatedEventData);
          break;
        case 'AccountUpdatedEvent':
          handlers.onAccountUpdated?.(msg.data as AccountUpdatedEventData);
          break;
      }
    };
  }

  connect();

  return {
    close: () => {
      closedByUser = true;
      clearTimers();
      try {
        ws?.close();
      } catch {
        // ignore
      }
      ws = null;
    },
  };
}
