"""OKX requires text ping/pong, rather than WebSocket control-frame pings."""
import threading
from urllib.parse import urlsplit
import websocket


def install_okx_heartbeat():
    original=websocket.WebSocketApp.run_forever
    if getattr(original,"_crypto_text_heartbeat",False):return
    def run_forever(app,*args,**kwargs):
        if urlsplit(app.url).hostname!="ws.okx.com":return original(app,*args,**kwargs)
        stopped=threading.Event();opened=app.on_open;message=app.on_message
        def beat():
            while not stopped.wait(20):
                try:app.send("ping")
                except Exception:return
        def on_open(connection):
            if opened:opened(connection)
            threading.Thread(target=beat,daemon=True,name="okx-text-heartbeat").start()
        def on_message(connection,payload):
            if payload not in ("pong",b"pong") and message:message(connection,payload)
        app.on_open=on_open;app.on_message=on_message
        kwargs.update(ping_interval=0,ping_timeout=None)
        try:return original(app,*args,**kwargs)
        finally:
            stopped.set();app.on_open=opened;app.on_message=message
    run_forever._crypto_text_heartbeat=True
    websocket.WebSocketApp.run_forever=run_forever
