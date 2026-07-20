import {
  PHASES,
  createGame,
  startTurn,
  beginAttack,
  resolveDefense,
  endTurn,
  passTurn,
  applyPassDraw,
  HAND_LIMIT,
  handOverflowCount,
  discardHandOverflow,
  shouldOfferPassDraw,
  validateAttack,
  clearSave,
} from "./game.js?v=18-hidden-details";

const MQTT_MODULE_URL = "https://esm.run/mqtt@5.15.2";
const MQTT_BROKER_URL = "wss://broker.hivemq.com:8884/mqtt";
const TOPIC_ROOT = "prime-duel-online/v18";
const AUTO_TURN_DELAY = 1_500;
const META_HEARTBEAT_INTERVAL = 15_000;
const META_STALE_AFTER = 45_000;

const app = document.querySelector("#app");
const modalRoot = document.querySelector("#modal-root");

let game = null;
let selected = [];
let order = [];
let discardSelection = [];
const openGraveyards = new Set();
let myIndex = null;
let roomCode = "";
let myName = "";
let myRole = "player";
let isHost = false;

let mqttApi = null;
let mqttClient = null;
let clientId = "";
let hostClientId = null;
let opponentClientId = null;
let eventTopic = "";
let stateTopic = "";
let metaTopic = "";
let joinSent = false;
let roomTimer = null;
let reconnectTimer = null;
let autoTurnTimer = null;
let heartbeatTimer = null;
let hasConnectedOnce = false;
let networkState = "connecting";

const esc = (value) =>
  String(value).replace(
    /[&<>"']/g,
    (character) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        character
      ],
  );

function cardHtml(card, { isSelected = false, hidden = false, life = false } = {}) {
  const effect =
    card.type === "A"
      ? "使用時：1枚ドロー"
      : card.type === "B"
        ? "攻撃値 +1"
        : card.type === "C"
          ? "外部防御 -2"
          : "効果なし";
  return `<button type="button" class="card ${isSelected ? "selected" : ""} ${hidden ? "hidden" : ""} ${life ? "life-card" : ""}" data-card="${card.id}" ${hidden ? "disabled" : ""}>
    <span class="num">${card.number}</span><span class="type">${card.type}</span><span class="effect">${effect}</span>
  </button>`;
}

function graveCardHtml(card, { showType = true } = {}) {
  const typeClass = showType ? `type-${String(card.type).toLowerCase()}` : "type-hidden";
  const title = showType ? `${card.number}${card.type}` : String(card.number);
  return `<span class="grave-card ${typeClass}" title="${title}"><b>${card.number}</b>${showType ? `<small>${card.type}</small>` : ""}</span>`;
}

function title() {
  app.innerHTML = `<section class="hero"><div class="hero-card">
    <p class="eyebrow">ONLINE PRIME CARD GAME · DIRECT MQTT v18</p>
    <h1>PRIME<br>DUEL</h1>
    <p class="sub">ルームコードでつながる、2人用オンラインカードゲーム。<br>対戦への参加だけでなく、進行中のゲームも観戦できます。</p>
    <div class="form-row">
      <input class="input" id="name" maxlength="16" value="プレイヤー" aria-label="プレイヤー名">
      <input class="input room-input" id="room" maxlength="6" inputmode="text" autocapitalize="characters" autocomplete="off" autocorrect="off" spellcheck="false" enterkeyhint="go" placeholder="ルームコード" aria-label="ルームコード">
    </div>
    <div class="footer-actions">
      <button type="button" class="btn primary" id="create">ルームを作る</button>
      <button type="button" class="btn gold" id="join">ルームに参加</button>
      <button type="button" class="btn" id="spectate">観戦する</button>
      <button type="button" class="btn" data-open-rules>ルール</button>
    </div>
    <p id="lobby-error" class="error" hidden></p>
  </div></section>`;

  document.querySelector("#create").addEventListener("click", () => connectToRoom("create"));
  document.querySelector("#join").addEventListener("click", () => connectToRoom("join"));
  document.querySelector("#spectate").addEventListener("click", () => connectToRoom("spectate"));

  const roomInput = document.querySelector("#room");
  const sanitizeRoom = () => {
    roomInput.value = roomInput.value.toUpperCase().replace(/[^A-Z2-9]/g, "").slice(0, 6);
  };
  roomInput.addEventListener("blur", sanitizeRoom);
  roomInput.addEventListener("compositionend", sanitizeRoom);
}

async function loadMqtt() {
  if (mqttApi) return mqttApi;
  const module = await import(MQTT_MODULE_URL);
  mqttApi = module.default || module;
  if (typeof mqttApi.connect !== "function" && typeof module.connect === "function") {
    mqttApi = module;
  }
  if (typeof mqttApi.connect !== "function") throw new Error("MQTT connect API is unavailable");
  return mqttApi;
}

async function connectToRoom(mode) {
  const nameInput = document.querySelector("#name");
  const roomInput = document.querySelector("#room");
  myName = nameInput.value.trim() || (mode === "spectate" ? "観戦者" : "プレイヤー");
  const requestedCode = roomInput.value.trim().toUpperCase().replace(/[^A-Z2-9]/g, "");

  if (mode !== "create" && requestedCode.length !== 6) {
    lobbyError("6文字のルームコードを入力してください。");
    return;
  }

  isHost = mode === "create";
  myRole = isHost ? "host" : mode === "spectate" ? "spectator" : "player";
  myIndex = isHost ? 0 : mode === "join" ? 1 : null;
  roomCode = isHost ? makeRoomCode() : requestedCode;
  clientId = `pd-${crypto.randomUUID()}`;
  hostClientId = null;
  opponentClientId = null;
  joinSent = false;
  discardSelection = [];
  openGraveyards.clear();
  hasConnectedOnce = false;
  networkState = "connecting";

  const baseTopic = `${TOPIC_ROOT}/${roomCode}`;
  eventTopic = `${baseTopic}/events`;
  stateTopic = `${baseTopic}/state`;
  metaTopic = `${baseTopic}/meta`;

  waitingRoom("通信ライブラリを読み込んでいます…");

  try {
    const mqtt = await loadMqtt();
    mqttClient = mqtt.connect(MQTT_BROKER_URL, {
      clientId,
      clean: true,
      keepalive: 30,
      connectTimeout: 15_000,
      reconnectPeriod: 2_000,
      protocolVersion: 4,
      will: {
        topic: eventTopic,
        payload: JSON.stringify({ type: "leave", senderId: clientId, role: myRole }),
        qos: 1,
        retain: false,
      },
    });
    bindMqttEvents();
  } catch (error) {
    console.error(error);
    lobbyError("通信ライブラリを読み込めませんでした。ページを再読み込みしてください。");
  }
}

function bindMqttEvents() {
  mqttClient.on("connect", () => {
    const firstConnection = !hasConnectedOnce;
    hasConnectedOnce = true;
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
    setNetworkState("online");
    mqttClient.subscribe([metaTopic, stateTopic, eventTopic], { qos: 1 }, (error) => {
      if (error) {
        connectionModal("ルームの通信チャンネルを開けませんでした。少し待って再試行してください。");
        return;
      }

      if (isHost && firstConnection) {
        clearRetainedRoom(() => {
          publishMeta("waiting");
          startHeartbeat();
          waitingRoom("ルームを作成しました。相手の参加を待っています…");
        });
      } else if (isHost) {
        publishMeta(game ? "playing" : "waiting");
        startHeartbeat();
        if (game) {
          render();
          sync();
        } else {
          waitingRoom("通信を再接続しました。相手の参加を待っています…");
        }
      } else if (firstConnection) {
        waitingRoom(
          myRole === "spectator"
            ? "観戦するルームを探しています…"
            : "ルームを探しています…",
        );
        clearTimeout(roomTimer);
        roomTimer = setTimeout(() => {
          if (!hostClientId && !game) {
            connectionModal("ルームが見つかりません。コードとホスト側の画面を確認してください。");
          }
        }, 20_000);
      } else if (game) {
        render();
        sync();
      } else {
        waitingRoom("通信を再接続しました。ルームを確認しています…");
      }
    });
  });

  mqttClient.on("message", (topic, payload) => {
    if (!payload?.length) return;
    let message;
    try {
      message = JSON.parse(payload.toString());
    } catch {
      return;
    }
    if (!message || message.senderId === clientId) return;

    if (topic === metaTopic && message.type === "meta") {
      receiveMeta(message);
    } else if (topic === stateTopic && message.type === "state") {
      receiveState(message.state);
    } else if (topic === eventTopic) {
      receiveEvent(message);
    }
  });

  mqttClient.on("reconnect", () => {
    setNetworkState("reconnecting");
    setConnectionText("通信を再接続しています…");
    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectionModal("通信を再接続できません。インターネット接続を確認してください。");
      }, 20_000);
    }
  });

  mqttClient.on("offline", () => {
    setNetworkState("reconnecting");
    setConnectionText("通信が一時的に切れました。再接続しています…");
  });
  mqttClient.on("error", (error) => console.error("MQTT error", error));
}

function clearRetainedRoom(done) {
  let remaining = 2;
  const next = () => {
    remaining -= 1;
    if (remaining === 0) done();
  };
  mqttClient.publish(metaTopic, "", { qos: 1, retain: true }, next);
  mqttClient.publish(stateTopic, "", { qos: 1, retain: true }, next);
}

function receiveMeta(message) {
  if (!message.sentAt || Date.now() - message.sentAt > META_STALE_AFTER) return;
  hostClientId = message.hostId;
  clearTimeout(roomTimer);
  if (isHost || joinSent) return;
  joinSent = true;
  publishEvent({ type: "join", role: myRole, name: myName });
  waitingRoom(
    myRole === "spectator"
      ? message.status === "playing"
        ? "対戦画面を受信しています…"
        : "対戦の開始を待っています…"
      : "ホストに参加を申請しています…",
  );
}

function receiveEvent(message) {
  if (message.targetId && message.targetId !== clientId) return;

  if (message.type === "rematch-ready") {
    receiveRematchReady(message);
    return;
  }

  if (message.type === "join" && isHost) {
    if (message.role === "spectator") {
      if (game) sync();
      else publishEvent({ type: "notice", targetId: message.senderId, notice: "waiting" });
      return;
    }
    if (message.role !== "player") return;
    if (message.senderId === opponentClientId) {
      if (game) sync();
      return;
    }
    if (game || opponentClientId) {
      publishEvent({ type: "notice", targetId: message.senderId, notice: "room-full" });
      return;
    }

    opponentClientId = message.senderId;
    game = createGame([myName, String(message.name || "プレイヤー").slice(0, 16)]);
    startTurn(game);
    publishMeta("playing");
    render();
    sync();
    return;
  }

  if (message.type === "notice") {
    if (message.notice === "room-full") {
      connectionModal("このルームの対戦枠は満員です。「観戦する」から入り直してください。");
    } else if (message.notice === "waiting") {
      waitingRoom("対戦の開始を待っています…");
    }
    return;
  }

  if (message.type === "leave") {
    const opponentLeft =
      message.senderId === opponentClientId ||
      (!isHost && message.senderId === hostClientId) ||
      (myRole === "spectator" && (message.role === "host" || message.role === "player"));
    if (opponentLeft) connectionModal("対戦プレイヤーがルームから退出しました。");
  }
}

function publishEvent(data) {
  if (!mqttClient?.connected) return;
  mqttClient.publish(
    eventTopic,
    JSON.stringify({ ...data, senderId: clientId, role: myRole, sentAt: Date.now() }),
    { qos: 1, retain: false },
  );
}

function publishMeta(status) {
  if (!mqttClient?.connected || !isHost) return;
  mqttClient.publish(
    metaTopic,
    JSON.stringify({
      type: "meta",
      senderId: clientId,
      hostId: clientId,
      hostName: myName,
      status,
      sentAt: Date.now(),
    }),
    { qos: 1, retain: true },
  );
}

function startHeartbeat() {
  clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(() => publishMeta(game ? "playing" : "waiting"), META_HEARTBEAT_INTERVAL);
}

function receiveRematchReady(message) {
  if (!game || game.phase !== PHASES.GAME_OVER || nextOverflowPlayerIndex() >= 0) return;
  const readyIndex = message.role === "host" ? 0 : message.role === "player" ? 1 : null;
  if (readyIndex === null) return;
  if (isHost && (readyIndex !== 1 || message.senderId !== opponentClientId)) return;
  if (myRole === "player" && (readyIndex !== 0 || message.senderId !== hostClientId)) return;

  game.rematchReady ??= [false, false];
  game.rematchReady[readyIndex] = true;
  gameOver();

  if (isHost) {
    if (game.rematchReady.every(Boolean)) startRematch();
    else sync();
  }
}

function requestRematch() {
  if (!game || game.phase !== PHASES.GAME_OVER || myIndex === null || nextOverflowPlayerIndex() >= 0) return;
  game.rematchReady ??= [false, false];
  if (game.rematchReady[myIndex]) return;
  game.rematchReady[myIndex] = true;
  publishEvent({ type: "rematch-ready" });
  gameOver();

  if (isHost) {
    if (game.rematchReady.every(Boolean)) startRematch();
    else sync();
  }
}

function startRematch() {
  if (!isHost || !game || game.phase !== PHASES.GAME_OVER || nextOverflowPlayerIndex() >= 0) return;
  const names = game.players.map((player) => player.name);
  game = createGame(names);
  startTurn(game);
  selected = [];
  order = [];
  discardSelection = [];
  openGraveyards.clear();
  modalRoot.innerHTML = "";
  publishMeta("playing");
  render();
  sync();
}

function receiveState(state) {
  if (!state?.players || !Array.isArray(state.players) || state.players.length !== 2) return;
  clearTimeout(autoTurnTimer);
  autoTurnTimer = null;
  clearTimeout(roomTimer);
  game = state;
  modalRoot.innerHTML = "";
  selected = [];
  order = [];
  render();
  handleMandatoryActions();
}

function sync() {
  if (!mqttClient?.connected || !game || myRole === "spectator") return;
  mqttClient.publish(
    stateTopic,
    JSON.stringify({ type: "state", senderId: clientId, role: myRole, state: game, sentAt: Date.now() }),
    { qos: 1, retain: true },
    (error) => {
      if (error) connectionModal("ゲームデータを送信できませんでした。通信を確認してください。");
    },
  );
}

function makeRoomCode() {
  const characters = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return Array.from(bytes, (number) => characters[number % characters.length]).join("");
}

function lobbyError(message) {
  const element = document.querySelector("#lobby-error");
  if (element) {
    element.hidden = false;
    element.textContent = message;
  } else {
    connectionModal(message);
  }
}

function waitingRoom(message = "相手の参加を待っています…") {
  app.innerHTML = `<section class="hero"><div class="hero-card">
    <p class="eyebrow">ROOM ${isHost ? "CREATED" : "CONNECTION"}</p>
    <h2 id="connection-text">${esc(message)}</h2>
    <div class="prime-display"><div><div class="prime-number">${roomCode}</div><div class="equation">このコードを同じルームに入る人へ伝えてください</div></div></div>
    <div class="footer-actions">
      <button type="button" class="btn" id="copy">コードをコピー</button>
      <button type="button" class="btn" data-open-rules>ルール</button>
    </div>
  </div></section>`;
  document.querySelector("#copy")?.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(roomCode);
      setConnectionText("ルームコードをコピーしました。");
    } catch {
      setConnectionText(`ルームコード: ${roomCode}`);
    }
  });
}

function setConnectionText(message) {
  const element = document.querySelector("#connection-text");
  if (element) element.textContent = message;
}

function setNetworkState(state) {
  networkState = state;
  const element = document.querySelector("#network-state");
  if (!element) return;
  element.className = `badge network-status ${state}`;
  element.textContent = state === "online" ? "● 接続済み" : state === "reconnecting" ? "● 再接続中" : "● 接続中";
}

function connectionModal(message) {
  clearTimeout(roomTimer);
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal">
    <p class="eyebrow">CONNECTION</p><h2>${esc(message)}</h2>
    <button type="button" class="btn primary" id="back-title">タイトルへ</button>
  </div></div>`;
  document.querySelector("#back-title").addEventListener("click", () => {
    leaveNetwork();
    game = null;
    modalRoot.innerHTML = "";
    title();
  });
}

function rulesModal() {
  modalRoot.innerHTML = `<div class="modal-backdrop rules-backdrop" data-dismissible><div class="modal rules-modal" role="dialog" aria-modal="true" aria-labelledby="rules-title">
    <div class="rules-toolbar"><button type="button" class="btn" data-close-modal>← 戻る</button></div>
    <p class="eyebrow">HOW TO PLAY</p><h2 id="rules-title">3枚までの合計で素数攻撃</h2>
    <div class="rule-grid">
      <div><b>攻撃</b><p>手札から1〜3枚を選び、数字の合計が素数なら攻撃できます。1〜3は枚数制限なし、4〜6と7〜13はそれぞれ1枚までです。</p></div>
      <div><b>防御とダメージ</b><p>手札1〜2枚の合計、または外部カードで防御します。攻撃値との差がダメージ枚数となり、残ったライフの小さい番号から順に手札へ移します。結果表示後は自動で次のターンへ進みます。</p></div>
      <div><b>カード効果</b><p>Aは使用枚数分ドロー、Bは攻撃値+1、Cは外部防御値-2です。</p></div>
      <div><b>パス</b><p>いつでもパスできます。次の自分のターンは通常ドローを行わず、代わりに山札上の2枚から1枚を選んで引きます。</p></div>
      <div><b>手札上限</b><p>手札は13枚までです。14枚以上になった場合、13枚になるよう超過分を選んで墓地へ送ります。</p></div>
      <div><b>観戦</b><p>観戦者は両プレイヤーの手札を見られますが、ゲーム操作はできません。</p></div>
      <div><b>勝利</b><p>相手のライフカードをすべて手札へ移動させると勝利です。</p></div>
    </div>
    <button type="button" class="btn primary rules-bottom-back" data-close-modal>戻る</button>
  </div></div>`;
  modalRoot.querySelector("[data-close-modal]")?.focus({ preventScroll: true });
}

function leaveNetwork() {
  clearTimeout(roomTimer);
  clearTimeout(reconnectTimer);
  clearTimeout(autoTurnTimer);
  clearInterval(heartbeatTimer);
  autoTurnTimer = null;
  heartbeatTimer = null;
  if (!mqttClient) return;
  if (mqttClient.connected) {
    publishEvent({ type: "leave" });
    if (isHost) {
      mqttClient.publish(metaTopic, "", { qos: 1, retain: true });
      mqttClient.publish(stateTopic, "", { qos: 1, retain: true });
    }
  }
  mqttClient.end(false);
  mqttClient = null;
}

function handoff(after) {
  after?.();
  sync();
  handleMandatoryActions();
}

const operatorIndex = () => (game.phase === PHASES.DEFENSE ? 1 - game.active : game.active);
const shouldShowPassDraw = () =>
  myRole !== "spectator" &&
  shouldOfferPassDraw(game, myIndex);
const nextOverflowPlayerIndex = () => game?.players.findIndex((player) => handOverflowCount(player) > 0) ?? -1;

function handleMandatoryActions() {
  if (!game) return;
  const overflowIndex = nextOverflowPlayerIndex();
  if (overflowIndex >= 0) {
    clearTimeout(autoTurnTimer);
    autoTurnTimer = null;
    if (myRole !== "spectator" && myIndex === overflowIndex) discardOverflowModal(overflowIndex);
    return;
  }
  if (game.phase === PHASES.GAME_OVER) return;
  if (shouldShowPassDraw()) {
    passDrawModal();
    return;
  }
  maybeScheduleAutomaticTurn();
}

function maybeScheduleAutomaticTurn() {
  if (
    autoTurnTimer ||
    game?.phase !== PHASES.RESULT ||
    myRole === "spectator" ||
    myIndex !== 1 - game.active
  ) return;
  const resolvedTurn = game.turn;
  autoTurnTimer = setTimeout(() => {
    autoTurnTimer = null;
    if (!game || game.phase !== PHASES.RESULT || game.turn !== resolvedTurn || nextOverflowPlayerIndex() >= 0) return;
    endTurn(game);
    render();
    sync();
    handleMandatoryActions();
  }, AUTO_TURN_DELAY);
}

function playerPanel(player, index) {
  const active = index === operatorIndex();
  const visible = myRole === "spectator" || index === myIndex;
  const hand = visible
    ? `<h3>${myRole === "spectator" ? "公開手札" : "あなたの手札"}</h3><div class="cards">${
        player.hand
          .map((card) => cardHtml(card, { isSelected: selected.includes(card.id) }))
          .join("") || '<p class="sub">手札がありません</p>'
      }</div>`
    : `<h3>相手の手札</h3><div class="cards">${player.hand
        .map((card) => cardHtml(card, { hidden: true }))
        .join("")}</div>`;

  const showGraveTypes = myRole === "spectator" || index === myIndex;
  const graveyard = player.graveyard.length
    ? player.graveyard.map((card) => graveCardHtml(card, { showType: showGraveTypes })).join("")
    : '<span class="muted-small">墓地にカードはありません</span>';

  return `<section class="player ${active ? "active" : ""}">
    <h2>${esc(player.name)} ${active ? '<span class="badge">操作中</span>' : ""}</h2>
    <div class="stats">
      <div class="stat"><strong>${player.deck.length}</strong><span>山札</span></div>
      <div class="stat"><strong>${player.hand.length}</strong><span>手札</span></div>
      <div class="stat"><strong>${player.graveyard.length}</strong><span>墓地</span></div>
      <div class="stat"><strong>${player.lifeZone.length}</strong><span>残りライフ</span></div>
    </div>
    <div class="life">${player.lifeZone.map((card) => `<i>${card.number}</i>`).join("")}</div>
    <details class="graveyard-view" data-graveyard="${index}" ${openGraveyards.has(index) ? "open" : ""}><summary>墓地を見る <span>${player.graveyard.length}枚</span></summary><div class="graveyard-cards">${graveyard}</div></details>
    ${hand}
  </section>`;
}

function render() {
  if (!game) {
    title();
    return;
  }
  const actingPlayer = game.players[operatorIndex()];
  const validation = game.phase === PHASES.ATTACK ? validateAttack(order) : null;
  const networkLabel = networkState === "online" ? "● 接続済み" : networkState === "reconnecting" ? "● 再接続中" : "● 接続中";
  const externalTotal = game.external.length + game.externalGrave.length;
  const usedExternal = game.externalGrave.length
    ? game.externalGrave.map((number) => `<i>${number}</i>`).join("")
    : '<span class="muted-small">なし</span>';
  app.innerHTML = `<div class="shell">
    <header class="topbar">
      <div class="brand">PRIME <b>DUEL</b></div>
      <div class="status-group"><span class="badge">ROOM ${roomCode}</span> ${myRole === "spectator" ? '<span class="badge">観戦中</span>' : ""} <span class="badge turn">TURN ${game.turn}</span> <span class="badge">${phaseName()}</span> <span id="network-state" class="badge network-status ${networkState}">${networkLabel}</span></div>
      <button type="button" class="btn" data-open-rules>ルール</button>
    </header>
    <div class="grid">
      ${playerPanel(game.players[0], 0)}
      <section class="panel board-center">
        <p class="eyebrow">${esc(actingPlayer.name)}'S ACTION</p><h2>${instruction()}</h2>
        <div class="prime-display">${centerDisplay(validation)}</div>
        <div class="zone-summary"><div><span>外部ゾーン</span><strong>${game.external.length} / ${externalTotal}</strong></div><div><span>使用済み</span><span class="used-external">${usedExternal}</span></div></div>
        ${actionControls(validation)}
        <h3>ゲームログ</h3><div class="log">${game.logs.map((entry) => `<div>${esc(entry)}</div>`).join("")}</div>
        <div class="footer-actions"><button type="button" class="btn danger" id="restart">ルーム退出</button></div>
      </section>
      ${playerPanel(game.players[1], 1)}
    </div>
  </div>`;
  bindGameControls();
  document.querySelectorAll("[data-graveyard]").forEach((details) => {
    details.addEventListener("toggle", () => {
      const index = Number(details.dataset.graveyard);
      if (details.open) openGraveyards.add(index);
      else openGraveyards.delete(index);
    });
  });
  const log = document.querySelector(".log");
  if (log) log.scrollTop = log.scrollHeight;
  if (game.phase === PHASES.GAME_OVER) gameOver();
}

function phaseName() {
  return game.phase === PHASES.ATTACK
    ? "攻撃カード選択"
    : game.phase === PHASES.DEFENSE
      ? "防御方法選択"
      : game.phase === PHASES.RESULT
        ? "ターン結果"
        : "勝敗決定";
}

function instruction() {
  return game.phase === PHASES.ATTACK
    ? "1〜3枚を選び、数字の合計で素数を作る"
    : game.phase === PHASES.DEFENSE
      ? "手札か外部ゾーンで防御する"
      : game.phase === PHASES.RESULT
        ? "攻防の結果"
        : "ゲーム終了";
}

function centerDisplay(validation) {
  if (game.phase === PHASES.ATTACK) {
    const total = order.reduce((sum, card) => sum + card.number, 0);
    const formula = order.length ? `${order.map((card) => card.number).join(" + ")} = ${total}` : "—";
    return `<div><div class="prime-number">${total || "—"}</div><div class="equation">${formula}</div><div class="equation">${validation?.message || "カードを1〜3枚選択してください"}</div></div>`;
  }
  if (game.phase === PHASES.DEFENSE) {
    const defender = game.players[1 - game.active];
    const defenseCards = defender.hand.filter((card) => selected.includes(card.id));
    const defenseValue = defenseCards.reduce((sum, card) => sum + card.number, 0);
    const preview = defenseCards.length
      ? `<div class="equation defense-preview">防御候補 ${defenseCards.map((card) => card.number).join(" + ")} = ${defenseValue} ／ 予想ダメージ ${Math.max(0, game.attack.value - defenseValue)}</div>`
      : '<div class="equation">防御カードを1〜2枚選ぶか、外部ゾーンを使います</div>';
    return `<div><div class="prime-number">${game.attack.value}</div><div class="equation">確定した攻撃値</div>${preview}</div>`;
  }
  if (game.lastResult) {
    return `<div><div class="prime-number">${game.lastResult.damage}</div><div class="equation">攻撃 ${game.lastResult.attack} − 防御 ${game.lastResult.defense}</div><div class="equation result-moved">ライフ移動：${game.lastResult.moved.join(", ") || "なし"}</div></div>`;
  }
  return "";
}

function actionControls(validation) {
  const overflowIndex = nextOverflowPlayerIndex();
  if (overflowIndex >= 0) {
    return `<p class="hint hand-limit-wait">${esc(game.players[overflowIndex].name)}が手札を${HAND_LIMIT}枚に調整しています…</p>`;
  }
  if (game.phase === PHASES.RESULT) {
    return `<div class="auto-turn"><span class="auto-turn-dot"></span><b>次のターンへ自動で進みます</b></div><p class="hint">ライフ移動: ${game.lastResult.moved.join(", ") || "なし"} ／ Aドロー: ${game.lastResult.drawn}枚</p>`;
  }
  if (myRole === "spectator") {
    return `<p class="hint">観戦中です。${esc(game.players[operatorIndex()].name)}の操作を表示しています。</p>`;
  }
  if (operatorIndex() !== myIndex) {
    return `<p class="hint">${esc(game.players[operatorIndex()].name)}の操作を待っています…</p>`;
  }
  if (game.phase === PHASES.ATTACK) {
    return `<div class="actions"><button type="button" class="btn primary" id="attack" ${!validation?.isValid ? "disabled" : ""}>攻撃確定</button><button type="button" class="btn gold" id="pass">パス</button></div>${validation && !validation.isValid && order.length ? `<p class="error">${esc(validation.message)}</p>` : ""}`;
  }
  if (game.phase === PHASES.DEFENSE) {
    const defender = game.players[1 - game.active];
    const defenseValue = defender.hand
      .filter((card) => selected.includes(card.id))
      .reduce((sum, card) => sum + card.number, 0);
    return `<div class="actions"><button type="button" class="btn primary" id="defend-hand" ${selected.length < 1 || selected.length > 2 ? "disabled" : ""}>手札で防御${selected.length ? `（合計 ${defenseValue}）` : ""}</button><button type="button" class="btn gold" id="defend-ext" ${!game.external.length ? "disabled" : ""}>外部カードを使う</button></div><p class="hint">手札防御は1〜2枚の合計です。外部ゾーンは山の一番上から1枚を使い、使用後に数字が公開されます。</p>`;
  }
  return "";
}

function bindGameControls() {
  document.querySelector("#restart")?.addEventListener("click", () => {
    if (confirm("現在のルームから退出しますか？")) {
      leaveNetwork();
      game = null;
      title();
    }
  });
  document.querySelectorAll("[data-card]").forEach((element) => {
    element.addEventListener("click", () => toggleCard(element.dataset.card));
  });
  document.querySelector("#attack")?.addEventListener("click", () => {
    beginAttack(game, order);
    selected = [];
    order = [];
    handoff(render);
  });
  document.querySelector("#pass")?.addEventListener("click", () => {
    passTurn(game);
    selected = [];
    order = [];
    handoff(render);
  });
  document.querySelector("#defend-hand")?.addEventListener("click", () => {
    const cards = game.players[1 - game.active].hand.filter((card) => selected.includes(card.id));
    finishDefense("hand", cards);
  });
  document.querySelector("#defend-ext")?.addEventListener("click", () => {
    finishDefense("external");
  });
}

function finishDefense(method, cards = []) {
  resolveDefense(game, method, cards);
  selected = [];
  render();
  sync();
  handleMandatoryActions();
}

function toggleCard(cardId) {
  if (myRole === "spectator" || operatorIndex() !== myIndex) return;
  const player = game.players[operatorIndex()];
  if (game.phase === PHASES.ATTACK) {
    if (selected.includes(cardId)) {
      selected = selected.filter((id) => id !== cardId);
      order = order.filter((card) => card.id !== cardId);
    } else {
      const card = player.hand.find((candidate) => candidate.id === cardId);
      const tentative = [...order, card];
      const bandValidation = validateBandOnly(tentative);
      if (!bandValidation.ok) return flash(bandValidation.message);
      if (selected.length >= 3) return flash("攻撃カードは最大3枚です。");
      selected.push(cardId);
      order.push(card);
    }
  } else if (game.phase === PHASES.DEFENSE) {
    if (selected.includes(cardId)) {
      selected = selected.filter((id) => id !== cardId);
    } else if (selected.length < 2) {
      selected.push(cardId);
    } else {
      return flash("防御カードは最大2枚です。");
    }
  }
  render();
}

function validateBandOnly(cards) {
  const high = cards.filter((card) => card.number >= 7).length;
  const middle = cards.filter((card) => card.number >= 4 && card.number <= 6).length;
  if (high > 1) return { ok: false, message: "7〜13のカードは1枚までです。" };
  if (middle > 1) return { ok: false, message: "4〜6のカードは1枚までです。" };
  return { ok: true };
}

function flash(message) {
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal"><p class="error">${esc(message)}</p><button type="button" class="btn primary" data-close-modal>確認</button></div></div>`;
}

function discardOverflowModal(playerIndex) {
  const player = game.players[playerIndex];
  const required = handOverflowCount(player);
  if (!required) {
    discardSelection = [];
    modalRoot.innerHTML = "";
    handleMandatoryActions();
    return;
  }

  const currentModal = modalRoot.querySelector("[data-discard-player]");
  if (!currentModal || Number(currentModal.dataset.discardPlayer) !== playerIndex) discardSelection = [];
  const validIds = new Set(player.hand.map((card) => card.id));
  discardSelection = discardSelection.filter((id) => validIds.has(id)).slice(0, required);

  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal discard-modal" data-discard-player="${playerIndex}" role="dialog" aria-modal="true" aria-labelledby="discard-title">
    <p class="eyebrow">HAND LIMIT</p><h2 id="discard-title">手札を${HAND_LIMIT}枚にする</h2>
    <p class="sub">手札が${player.hand.length}枚あります。墓地へ送るカードを<b>${required}枚</b>選んでください。</p>
    <div class="discard-counter">選択中 ${discardSelection.length} / ${required}枚</div>
    <div class="choice-cards discard-cards">${player.hand.map((card) => cardHtml(card, { isSelected: discardSelection.includes(card.id) })).join("")}</div>
    <button type="button" class="btn primary" id="confirm-discard" ${discardSelection.length !== required ? "disabled" : ""}>選んだカードを墓地へ送る</button>
  </div></div>`;

  modalRoot.querySelectorAll("[data-card]").forEach((element) => {
    element.addEventListener("click", () => {
      const cardId = element.dataset.card;
      if (discardSelection.includes(cardId)) discardSelection = discardSelection.filter((id) => id !== cardId);
      else if (discardSelection.length < required) discardSelection.push(cardId);
      discardOverflowModal(playerIndex);
    });
  });

  document.querySelector("#confirm-discard")?.addEventListener("click", () => {
    const result = discardHandOverflow(game, playerIndex, discardSelection);
    if (!result.ok) {
      discardSelection = [];
      discardOverflowModal(playerIndex);
      return;
    }
    discardSelection = [];
    modalRoot.innerHTML = "";
    render();
    sync();
    handleMandatoryActions();
  });
}

function passDrawModal() {
  const player = game.players[game.active];
  const choices = player.deck.slice(0, 2);
  if (!choices.length) {
    player.hasPassDrawBonus = false;
    render();
    sync();
    handleMandatoryActions();
    return;
  }
  if (choices.length === 1) {
    applyPassDraw(game, choices[0].id);
    render();
    sync();
    handleMandatoryActions();
    return;
  }
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal">
    <p class="eyebrow">PASS SELECT DRAW</p><h2>通常ドローの代わりに1枚を選ぶ</h2>
    <p class="sub">選ばなかったカードは山札の一番下へ移動します。</p>
    <div class="choice-cards">${choices.map((card) => cardHtml(card)).join("")}</div>
  </div></div>`;
  modalRoot.querySelectorAll("[data-card]").forEach((element) => {
    element.addEventListener("click", () => {
      applyPassDraw(game, element.dataset.card);
      modalRoot.innerHTML = "";
      render();
      sync();
      handleMandatoryActions();
    });
  });
}

function gameOver() {
  clearSave();
  const winner = game.players[game.winner];
  const overflowIndex = nextOverflowPlayerIndex();
  const ready = game.rematchReady || [false, false];
  const myReady = myIndex !== null && ready[myIndex];
  const opponentReady = myIndex !== null && ready[1 - myIndex];
  const rematchStatus = overflowIndex >= 0
    ? `${esc(game.players[overflowIndex].name)}が手札を${HAND_LIMIT}枚に調整しています…`
    : myRole === "spectator"
    ? "プレイヤーが再戦を選ぶと、同じルームで次の対戦が始まります。"
    : myReady
      ? opponentReady
        ? "再戦を開始します…"
        : "相手の再戦準備を待っています…"
      : opponentReady
        ? "相手が再戦を希望しています。"
        : "両プレイヤーが再戦を選ぶと、同じルームで続けられます。";
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal">
    <p class="eyebrow">GAME OVER</p><h1>${esc(winner.name)}<br>WIN</h1>
    <p class="sub">相手のライフゾーンをすべて攻略しました。</p>
    <p class="hint rematch-status">${rematchStatus}</p>
    <div class="footer-actions">
      ${myRole === "spectator" || overflowIndex >= 0 ? "" : `<button type="button" class="btn primary" id="rematch" ${myReady ? "disabled" : ""}>${myReady ? "再戦準備完了" : "同じ相手と再戦"}</button>`}
      <button type="button" class="btn" id="back-title">タイトルへ</button>
    </div>
  </div></div>`;
  document.querySelector("#rematch")?.addEventListener("click", requestRematch);
  document.querySelector("#back-title").addEventListener("click", () => {
    leaveNetwork();
    game = null;
    modalRoot.innerHTML = "";
    title();
  });
}

document.addEventListener("click", (event) => {
  if (event.target.closest("[data-open-rules]")) {
    event.preventDefault();
    rulesModal();
  }
  if (event.target.closest("[data-close-modal]")) {
    event.preventDefault();
    modalRoot.innerHTML = "";
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && modalRoot.querySelector("[data-dismissible]")) modalRoot.innerHTML = "";
});

window.addEventListener("beforeunload", leaveNetwork);
title();
