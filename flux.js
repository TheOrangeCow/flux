// Browser client for flux. Usage:
//   const c = new Flux("ws://localhost:5000");
//   c.on("players", data => ...);  c.emit("move", {x: 1, y: 2});
class Flux {
    constructor(url, { token = null, reconnect = true } = {}) {
        Object.assign(this, { url, token, reconnect, handlers: {}, pending: [], ready: false, id: null, delay: 1000 });
        this._open();
    }
    on(event, fn) { (this.handlers[event] ||= []).push(fn); return this; }
    emit(event, data = null) {
        const msg = JSON.stringify({ event, data });
        if (this.ready) this.ws.send(msg); else this.pending.push(msg);
    }
    close() { this.reconnect = false; this.ws.close(); }
    _fire(event, ...args) { (this.handlers[event] || []).forEach(f => f(...args)); }
    _open() {
        const ws = this.ws = new WebSocket(this.url);
        ws.onopen = () => { this.delay = 1000; if (this.token !== null) ws.send(JSON.stringify({ event: "auth", data: this.token })); };
        ws.onmessage = e => {
            let m; try { m = JSON.parse(e.data); } catch { m = { event: "message", data: e.data }; }
            if (m.event === "__ping") return ws.send(JSON.stringify({ event: "__pong" }));
            if (m.event === "welcome") {
                this.id = m.data.id; this.ready = true;
                this.pending.splice(0).forEach(p => ws.send(p));
                this._fire("connect");
            }
            this._fire("*", m.event, m.data);
            this._fire(m.event, m.data);
        };
        ws.onclose = () => {
            if (this.ready) this._fire("disconnect");
            this.ready = false;
            if (this.reconnect) setTimeout(() => this._open(), this.delay = Math.min(this.delay * 2, 15000));
        };
    }
}
