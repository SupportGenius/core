/*! SupportGenius widget v1.0.0 | https://supportgeni.us */
// Dependency-free support chat in one classic script (ES2017, no build step): a Shadow
// DOM panel that talks to the SupportGenius API on this script's own origin using
// CORS-"simple" requests only. Its only global is the init flag.
(function () {
  'use strict';

  // Capture the tag synchronously (async scripts still see currentScript here), read
  // the config off it once, and refuse to double-initialise or to run inline.
  var script = document.currentScript;
  if (window.__sgWidget) return;
  if (!script || !script.src) {
    console.error('[SupportGenius] w.js must be included via <script src="...">; widget not started.');
    return;
  }
  window.__sgWidget = true;

  var MAX_CHARS = 4000;     // composer hard cap
  var QUOTE_MAX = 160;      // citation quote truncation
  var POLL_BASE_MS = 10000; // poll interval while the panel is open
  var POLL_MAX_MS = 60000;  // error back-off ceiling
  var TURNSTILE_SRC = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
  var MSG_VERIFY = 'Verification is unavailable right now, please try again later.';

  // The API lives on the origin the script was served from (dev and production alike).
  var base = new URL(script.src).origin;
  var key = (script.getAttribute('data-key') || '').trim();
  var title = script.getAttribute('data-title') || 'Support';

  // Refuse anything but a publishable key before any network call; a secret key in a
  // page is a serious misconfiguration.
  if (key.indexOf('sg_pub_') !== 0) {
    console.error('[SupportGenius] data-key must be a publishable key ("sg_pub_..."). Widget not started.');
    if (key.indexOf('sg_') === 0) {
      console.error('[SupportGenius] WARNING: the configured data-key looks like a SECRET key. Secret keys must ' +
        'never be put in a page - revoke it and use a publishable key instead.');
    }
    return;
  }

  // Storage: localStorage with an in-memory fallback, namespaced by a prefix of the
  // publishable key so co-hosted tenants never share state.
  var NS = 'sg:' + key.slice(0, 16);
  var mem = {};
  var ls = null;
  try { // mere access can throw in some privacy modes
    ls = window.localStorage; ls.setItem('__sg_probe__', '1'); ls.removeItem('__sg_probe__');
  } catch (e) { ls = null; }

  function storeGet(k) {
    try { return ls ? ls.getItem(k) : (k in mem ? mem[k] : null); }
    catch (e) { return k in mem ? mem[k] : null; }
  }
  function storeSet(k, v) { // memory copy first, so a full/disabled store still works
    mem[k] = v;
    try { if (ls) ls.setItem(k, v); } catch (e) { /* privacy mode / quota */ }
  }
  function storeDel(k) {
    delete mem[k]; try { if (ls) ls.removeItem(k); } catch (e) { /* ignore */ }
  }

  // --- Runtime state ------------------------------------------------------------
  var state = {
    open: false, sending: false, escalated: false,
    renderedCount: 0, // server messages currently rendered in the list
    conv: null,    // {id, token} - token is the server's conversation_token
    visitor: null, // opaque signed string, replaced on every reply
    pollDelay: POLL_BASE_MS, pollTimer: 0, pollBusy: false,
    typingEl: null, turnstileState: 'idle', turnstileWidget: null
  };
  var refs = {};           // shadow-DOM element references
  var turnstileQueue = []; // callbacks waiting on the turnstile script

  // --- Styles (inline in the shadow root; host-page CSS cannot leak in) ----------
  var CSS = `
    :host{all:initial;display:block;position:fixed;bottom:20px;right:20px;z-index:2147483647;font-family:system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.45;color:#1f2430}
    *{box-sizing:border-box;margin:0;padding:0}button{font:inherit;cursor:pointer}[hidden]{display:none!important}
    .launcher{width:56px;height:56px;border:0;border-radius:50%;background:#2563eb;color:#fff;display:flex;align-items:center;justify-content:center;box-shadow:0 6px 20px rgba(0,0,0,.25)}.launcher:hover{background:#1d4ed8}.launcher svg{width:26px;height:26px;fill:currentColor}
    .panel{position:absolute;bottom:68px;right:0;width:340px;max-width:calc(100vw - 32px);height:480px;max-height:calc(100vh - 100px);display:flex;flex-direction:column;overflow:hidden;background:#fff;border:1px solid #e3e6ec;border-radius:14px;box-shadow:0 12px 40px rgba(0,0,0,.22)}
    .head{display:flex;align-items:center;justify-content:space-between;flex:none;padding:12px 14px;background:#2563eb;color:#fff}.title{font-weight:600}.close{background:none;border:0;color:#fff;font-size:20px;line-height:1;padding:2px 8px;border-radius:6px}.close:hover{background:rgba(255,255,255,.15)}
    .banner{flex:none;padding:8px 14px;font-size:13px;background:#fff7e0;color:#7a5200;border-bottom:1px solid #f0e2b0}
    .list{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:10px;background:#f7f8fa}
    .msg{max-width:82%;padding:9px 12px;border-radius:12px;white-space:pre-wrap;overflow-wrap:break-word}.user{align-self:flex-end;background:#2563eb;color:#fff;border-bottom-right-radius:4px}.assistant{align-self:flex-start;background:#fff;border:1px solid #e3e6ec;border-bottom-left-radius:4px}
    .note{align-self:center;max-width:90%;padding:6px 10px;font-size:12px;border-radius:8px;background:#f1f3f7;color:#4b5563}.sources-label{margin-top:8px;font-size:11px;font-weight:600;color:#6b7280}.sources{margin:4px 0 0;padding-left:16px;font-size:12px;color:#6b7280}
    .typing{display:flex;gap:4px;align-items:center;padding:13px 14px}.dot{width:6px;height:6px;border-radius:50%;background:#9aa3b2;animation:sg-blink 1.2s infinite}.dot:nth-child(2){animation-delay:.2s}.dot:nth-child(3){animation-delay:.4s}
    @keyframes sg-blink{0%,80%,100%{opacity:.25}40%{opacity:1}}
    .captcha{flex:none;padding:10px 14px;background:#fff;border-top:1px solid #e3e6ec}.captcha-note{margin-bottom:8px;font-size:12px;color:#6b7280}
    .composer{flex:none;display:flex;gap:8px;padding:10px;background:#fff;border-top:1px solid #e3e6ec}
    .input{flex:1;resize:none;max-height:120px;padding:8px 10px;font:inherit;border:1px solid #d4d9e2;border-radius:10px}.input:focus{outline:2px solid #2563eb;outline-offset:-1px}.send{flex:none;padding:0 14px;font-weight:600;border:0;border-radius:10px;background:#2563eb;color:#fff}.send:hover:not(:disabled){background:#1d4ed8}.send:disabled{opacity:.5;cursor:default}
  `;

  // --- DOM helpers (createElement / textContent only - never innerHTML) ----------
  function el(tag, cls, text) {
    var node = document.createElement(tag); if (cls) node.className = cls;
    if (text != null) node.textContent = text;
    return node;
  }
  function add(parent) { for (var i = 1; i < arguments.length; i++) parent.appendChild(arguments[i]); return parent; }
  function button(cls, text, label) {
    var b = el('button', cls, text); b.type = 'button'; b.setAttribute('aria-label', label); return b;
  }
  function svgEl(name, attrs) {
    var node = document.createElementNS('http://www.w3.org/2000/svg', name);
    for (var k in attrs) node.setAttribute(k, attrs[k]);
    return node;
  }
  function chatIcon() {
    return add(svgEl('svg', { viewBox: '0 0 24 24', 'aria-hidden': 'true' }),
      svgEl('path', { d: 'M4 2h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H8l-4 4V4c0-1.1.9-2 2-2z' }));
  }

  // --- UI construction ------------------------------------------------------------
  function buildUI() {
    var host = el('div'); host.setAttribute('data-sg-widget', '');
    var root = host.attachShadow({ mode: 'open' });
    var style = el('style'); style.textContent = CSS;

    var launcher = add(button('launcher', null, 'Open support chat'), chatIcon());
    var close = button('close', '×', 'Close chat');
    var head = add(el('div', 'head'), el('span', 'title', title), close);
    var banner = el('div', 'banner', 'A member of our team will follow up.'); banner.hidden = true;
    var list = el('div', 'list'); list.setAttribute('aria-live', 'polite');
    var captchaSlot = el('div');
    var captchaBox = add(el('div', 'captcha'), // hidden until the server demands a captcha
      el('div', 'captcha-note', 'Please verify that you are human to continue.'), captchaSlot);
    captchaBox.hidden = true;

    var input = document.createElement('textarea');
    input.className = 'input'; input.rows = 2; input.maxLength = MAX_CHARS;
    input.placeholder = 'Type a message…'; input.setAttribute('aria-label', 'Your message');
    var send = button('send', 'Send', 'Send message');

    var panel = el('div', 'panel');
    panel.setAttribute('role', 'dialog'); panel.setAttribute('aria-label', title);
    panel.hidden = true;
    add(panel, head, banner, list, captchaBox, add(el('div', 'composer'), input, send));
    add(root, style, launcher, panel);
    document.body.appendChild(host);

    launcher.addEventListener('click', function () { setOpen(!state.open); });
    close.addEventListener('click', function () { setOpen(false); });
    send.addEventListener('click', onSend);
    input.addEventListener('keydown', function (event) { // Enter sends; Shift+Enter a newline
      if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); onSend(); }
    });
    // Escape closes from anywhere; the keydown event crosses the shadow boundary.
    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && state.open) { setOpen(false); launcher.focus(); }
    });

    refs = { launcher: launcher, panel: panel, banner: banner, list: list,
      captchaBox: captchaBox, captchaSlot: captchaSlot, input: input, send: send };
  }

  function setOpen(open) {
    state.open = open;
    refs.panel.hidden = !open;
    refs.launcher.setAttribute('aria-label', open ? 'Close support chat' : 'Open support chat');
    if (open) { refs.input.focus(); if (state.conv) fetchConversation(false); } // refresh lifecycle now
  }

  // --- Transcript rendering ---------------------------------------------------------
  function scrollBottom() { refs.list.scrollTop = refs.list.scrollHeight; }
  function truncate(text, max) { return text.length > max ? text.slice(0, max - 1) + '…' : text; }
  function note(text) { refs.list.appendChild(el('div', 'note', text)); scrollBottom(); }
  function addUser(text) { refs.list.appendChild(el('div', 'msg user', text)); state.renderedCount++; scrollBottom(); }

  function addAssistant(message) {
    var text = message.body != null ? message.body : message.answer; // stored vs live reply
    var bubble = el('div', 'msg assistant', text == null ? '' : String(text));
    var citations = Array.isArray(message.citations) ? message.citations : [];
    if (citations.length) { // "Sources": retrieved quotes, or brain pages as links — never raw chunk text
      bubble.appendChild(el('div', 'sources-label', 'Sources'));
      var ul = el('ul', 'sources');
      for (var i = 0; i < citations.length; i++) {
        var c = citations[i] || {};
        if (c.url) { // a Living Brain citation: a link labelled with its title
          var li = el('li');
          var a = el('a', null, truncate(String(c.title || c.url), QUOTE_MAX));
          if (/^https?:\/\//.test(c.url)) { a.href = c.url; a.target = '_blank'; a.rel = 'noopener noreferrer'; }
          li.appendChild(a); ul.appendChild(li);
        } else {
          ul.appendChild(el('li', null, truncate(String(c.quote || ''), QUOTE_MAX)));
        }
      }
      bubble.appendChild(ul);
    }
    refs.list.appendChild(bubble);
    state.renderedCount++;
    if (message.outcome === 'handoff') setEscalated(true);
    scrollBottom();
  }

  // One-way latch: once escalated, the banner stays until the conversation is dropped.
  function setEscalated(on) { if (on && !state.escalated) { state.escalated = true; refs.banner.hidden = false; } }

  // No streaming by design: three dots are all the visitor sees during the POST.
  function showTyping() {
    hideTyping();
    var t = el('div', 'msg assistant typing'); t.setAttribute('aria-label', 'Support is typing');
    for (var i = 0; i < 3; i++) t.appendChild(el('span', 'dot'));
    state.typingEl = refs.list.appendChild(t);
    scrollBottom();
  }
  function hideTyping() {
    if (state.typingEl && state.typingEl.parentNode) state.typingEl.parentNode.removeChild(state.typingEl); state.typingEl = null;
  }

  // --- HTTP: CORS-"simple" requests only - no Authorization, no custom headers; the
  // POST body is a JSON string sent as text/plain and the key travels in body/query.
  async function readJson(res) { // bodies may be application/problem+json: parse loosely
    try {
      var parsed = JSON.parse(await res.text());
      return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (e) { return null; }
  }
  async function request(method, path, payload) {
    var opts = { method: method, credentials: 'omit' };
    if (payload !== undefined) {
      opts.headers = { 'Content-Type': 'text/plain;charset=UTF-8' }; opts.body = JSON.stringify(payload);
    }
    try {
      var res = await fetch(base + path, opts);
      return { status: res.status, data: await readJson(res) };
    } catch (e) { return { status: 0, data: null }; } // network failure / offline
  }

  // --- Sending --------------------------------------------------------------------------
  function onSend() {
    if (state.sending) return;
    var text = refs.input.value.trim().slice(0, MAX_CHARS);
    if (!text) return; // empty / whitespace-only is ignored
    refs.input.value = '';
    var payload = { key: key, message: text };
    if (state.conv) payload.conversation_id = state.conv.id;
    if (state.visitor) payload.visitor = state.visitor;
    deliver(payload, true);
  }

  // Send one message, echoing it locally unless it is a captcha retry (already shown).
  async function deliver(payload, echo) {
    if (echo) addUser(payload.message);
    state.sending = true;
    refs.send.disabled = refs.input.disabled = true;
    showTyping();
    var res = await request('POST', '/v1/support/widget/messages', payload);
    hideTyping();
    state.sending = false;
    refs.send.disabled = refs.input.disabled = false;
    handleReply(res, payload);
  }

  function handleReply(res, payload) {
    var data = res.data;
    if (res.status === 200 && data) { persist(data); addAssistant(data); return; }
    if (res.status === 403 && data && 'site_key' in data) { // server demands a captcha
      if (typeof data.site_key === 'string' && data.site_key) startCaptcha(data.site_key, payload);
      else note(MSG_VERIFY);
      return;
    }
    if (res.status === 429) return note('You\'re sending messages too quickly — please wait a moment.');
    if (res.status === 401 || res.status === 403) {
      console.error('[SupportGenius] Chat is unavailable (HTTP ' + res.status + '). Check that this page\'s ' +
        'origin is on the tenant\'s allowlist and that the publishable key is valid.');
    } else if (res.status === 0) console.error('[SupportGenius] Could not reach ' + base + '.');
    else console.error('[SupportGenius] Unexpected response (HTTP ' + res.status + ').');
    note('Chat is unavailable.');
  }

  function persist(data) {
    if (typeof data.visitor === 'string' && data.visitor) {
      storeSet(NS + ':visitor', state.visitor = data.visitor); // opaque signed string: always replace
    }
    if (data.conversation_id && data.conversation_token) {
      state.conv = { id: data.conversation_id, token: data.conversation_token };
      storeSet(NS + ':conv', JSON.stringify(state.conv));
    }
    if (data.outcome === 'handoff' || data.needs_escalation) setEscalated(true);
  }

  // --- Turnstile captcha (challenge script loaded at most once, rendered explicitly) ----
  function loadTurnstile(done) {
    if (window.turnstile) return done(true);
    if (state.turnstileState === 'failed') return done(false);
    turnstileQueue.push(done);
    if (state.turnstileState === 'loading') return;
    state.turnstileState = 'loading';
    var tag = document.createElement('script');
    tag.src = TURNSTILE_SRC; tag.async = true;
    tag.onload = tag.onerror = function () {
      state.turnstileState = window.turnstile ? 'ready' : 'failed';
      var queue = turnstileQueue, ok = !!window.turnstile; turnstileQueue = [];
      for (var i = 0; i < queue.length; i++) queue[i](ok);
    };
    document.head.appendChild(tag);
  }

  function removeCaptcha() {
    if (state.turnstileWidget !== null && window.turnstile) {
      try { window.turnstile.remove(state.turnstileWidget); } catch (e) { /* already gone */ }
    }
    state.turnstileWidget = null;
  }

  function startCaptcha(siteKey, payload) {
    refs.captchaBox.hidden = false;
    loadTurnstile(function (ok) {
      if (!ok) { refs.captchaBox.hidden = true; note(MSG_VERIFY); return; }
      removeCaptcha(); refs.captchaSlot.textContent = '';
      state.turnstileWidget = window.turnstile.render(refs.captchaSlot, {
        sitekey: siteKey,
        callback: function (token) { // verified: resend the very same message
          removeCaptcha(); refs.captchaBox.hidden = true;
          payload.captcha_token = token;
          deliver(payload, false);
        }
      });
    });
  }

  // --- Conversation restore + lifecycle polling -------------------------------------------
  // GET the full transcript; requests never overlap. 404/401/403 on a stored conversation
  // drop it and start fresh; other poll errors back off.
  async function fetchConversation(forPoll) {
    if (!state.conv || state.pollBusy) return;
    state.pollBusy = true;
    var res = await request('GET', '/v1/support/widget/conversations/' + encodeURIComponent(state.conv.id) +
      '?key=' + encodeURIComponent(key) + '&token=' + encodeURIComponent(state.conv.token));
    state.pollBusy = false;
    if (res.status === 200 && res.data && Array.isArray(res.data.messages)) {
      state.pollDelay = POLL_BASE_MS;
      renderTranscript(res.data);
    } else if (res.status === 404 || res.status === 401 || res.status === 403) {
      forgetConversation(); // stale or unauthorised
    } else if (forPoll) {
      state.pollDelay = Math.min(state.pollDelay * 2, POLL_MAX_MS); // error back-off
    }
  }

  // Rebuild the list from server truth, but only when something actually changed (and
  // never clobber an in-flight send, which appends its own bubbles).
  function renderTranscript(data) {
    if (state.sending) return;
    var escalated = data.status === 'escalated' || !!data.needs_escalation;
    if (data.messages.length === state.renderedCount && escalated === state.escalated) return;
    refs.list.textContent = '';
    state.renderedCount = 0;
    for (var i = 0; i < data.messages.length; i++) {
      var m = data.messages[i];
      if (!m || typeof m.body !== 'string') continue;
      if (m.role === 'user') addUser(m.body);
      else if (m.role === 'assistant') addAssistant(m);
    }
    setEscalated(escalated);
  }

  // 404/401/403 on a stored conversation: drop it and start fresh.
  function forgetConversation() {
    state.conv = null; state.escalated = false; state.renderedCount = 0; state.pollDelay = POLL_BASE_MS;
    refs.banner.hidden = true; refs.list.textContent = ''; storeDel(NS + ':conv');
  }

  // Poll only while the panel is open and a conversation exists; skip ticks while the
  // tab is hidden; back off (double, max 60 s) after errors.
  function schedulePoll() { if (!state.pollTimer) state.pollTimer = setTimeout(pollTick, state.pollDelay); }
  async function pollTick() {
    state.pollTimer = 0;
    if (state.open && state.conv && !document.hidden) await fetchConversation(true);
    schedulePoll();
  }
  // Refresh immediately (at the base interval) when the tab becomes visible again.
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden && state.open && state.conv) {
      state.pollDelay = POLL_BASE_MS; fetchConversation(true);
    }
  });

  // --- Boot ------------------------------------------------------------------------------
  // Restore the saved visitor and conversation (and the transcript) on page load.
  function restore() {
    try {
      state.visitor = storeGet(NS + ':visitor') || state.visitor;
      var saved = JSON.parse(storeGet(NS + ':conv') || 'null');
      if (saved && saved.id && saved.token) {
        state.conv = { id: saved.id, token: saved.token }; fetchConversation(false);
      }
    } catch (e) { /* corrupt storage: start fresh */ }
  }

  function boot() { buildUI(); restore(); schedulePoll(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
