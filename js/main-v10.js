import {
  PHASES,
  createGame,
  startTurn,
  beginAttack,
  resolveDefense,
  endTurn,
  passTurn,
  applyPassDraw,
  validateAttack,
  clearSave,
} from "./game.js?v=11-life-count";

const MQTT_MODULE_URL = "https://esm.run/mqtt@5.15.2";
const MQTT_BROKER_URL = "wss://broker.hivemq.com:8884/mqtt";
const TOPIC_ROOT = "prime-duel-online/v11";

const app = document.querySelector("#app");
const modalRoot = document.querySelector("#modal-root");

let game = null;
let selected = [];
let order = [];
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

function title() {
  app.innerHTML = `<section class="hero"><div class="hero-card">
    <p class="eyebrow">ONLINE PRIME CARD GAME · DIRECT MQTT v11</p>
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
    clearTimeout(reconnectTimer);
    mqttClient.subscribe([metaTopic, stateTopic, eventTopic], { qos: 1 }, (error) => {
      if (error) {
        connectionModal("ルームの通信チャンネルを開けませんでした。少し待って再試行してください。");
        return;
      }

      if (isHost) {
        clearRetainedRoom(() => {
          publishMeta("waiting");
          waitingRoom("ルームを作成しました。相手の参加を待っています…");
        });
      } else {
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
    setConnectionText("通信を再接続しています…");
    if (!reconnectTimer) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectionModal("通信を再接続できません。インターネット接続を確認してください。");
      }, 20_000);
    }
  });

  mqttClient.on("offline", () => setConnectionText("通信が一時的に切れました。再接続しています…"));
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

  if (message.type === "join" && isHost) {
    if (message.role === "spectator") {
      if (game) sync();
      else publishEvent({ type: "notice", targetId: message.senderId, notice: "waiting" });
      return;
    }
    if (message.role !== "player") return;
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

function receiveState(state) {
  if (!state?.players || !Array.isArray(state.players) || state.players.length !== 2) return;
  clearTimeout(roomTimer);
  game = state;
  modalRoot.innerHTML = "";
  selected = [];
  order = [];
  render();
  if (
    myIndex !== null &&
    operatorIndex() === myIndex &&
    game.players[myIndex].hasPassDrawBonus
  ) {
    passDrawModal();
  }
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
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal" role="dialog" aria-modal="true" aria-labelledby="rules-title">
    <p class="eyebrow">HOW TO PLAY</p><h2 id="rules-title">3枚までの合計で素数攻撃</h2>
    <div class="rule-grid">
      <div><b>攻撃</b><p>手札から1〜3枚を選び、数字の合計が素数なら攻撃できます。1〜3は枚数制限なし、4〜6と7〜13はそれぞれ1枚までです。</p></div>
      <div><b>防御とダメージ</b><p>手札1〜2枚の合計、または外部カードで防御します。攻撃値との差がダメージ枚数となり、残ったライフの小さい番号から順に手札へ移します。</p></div>
      <div><b>カード効果</b><p>Aは使用枚数分ドロー、Bは攻撃値+1、Cは外部防御値-2です。</p></div>
      <div><b>パス</b><p>いつでもパスできます。次の自分のターンに、山札上の2枚から1枚を選んで引きます。</p></div>
      <div><b>観戦</b><p>観戦者は両プレイヤーの手札を見られますが、ゲーム操作はできません。</p></div>
      <div><b>勝利</b><p>相手のライフカードをすべて手札へ移動させると勝利です。</p></div>
    </div>
    <button type="button" class="btn primary" data-close-modal>閉じる</button>
  </div></div>`;
  modalRoot.querySelector("[data-close-modal]")?.focus();
}

function leaveNetwork() {
  clearTimeout(roomTimer);
  clearTimeout(reconnectTimer);
  if (!mqttClient) return;
  if (mqttClient.connected) {
    publishEvent({ type: "leave" });
    if (isHost) {
      mqttClient.publish(metaTopic, "", { qos: 1, retain: true });
      mqttClient.publish(stateTopic, "", { qos: 1, retain: true });
    }
  }
  mqttClient.end(true);
  mqttClient = null;
}

function handoff(after) {
  after?.();
  sync();
}

const operatorIndex = () => (game.phase === PHASES.DEFENSE ? 1 - game.active : game.active);

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

  return `<section class="player ${active ? "active" : ""}">
    <h2>${esc(player.name)} ${active ? '<span class="badge">操作中</span>' : ""}</h2>
    <div class="stats">
      <div class="stat"><strong>${player.deck.length}</strong><span>山札</span></div>
      <div class="stat"><strong>${player.hand.length}</strong><span>手札</span></div>
      <div class="stat"><strong>${player.graveyard.length}</strong><span>墓地</span></div>
      <div class="stat"><strong>${player.lifeZone.length}</strong><span>残りライフ</span></div>
    </div>
    <div class="life">${player.lifeZone.map((card) => `<i>${card.number}</i>`).join("")}</div>
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
  app.innerHTML = `<div class="shell">
    <header class="topbar">
      <div class="brand">PRIME <b>DUEL</b></div>
      <div><span class="badge">ROOM ${roomCode}</span> ${myRole === "spectator" ? '<span class="badge">観戦中</span>' : ""} <span class="badge turn">TURN ${game.turn}</span> <span class="badge">${phaseName()}</span> <span class="badge">外部ゾーン ${game.external.length}</span></div>
      <button type="button" class="btn" data-open-rules>ルール</button>
    </header>
    <div class="grid">
      ${playerPanel(game.players[0], 0)}
      <section class="panel board-center">
        <p class="eyebrow">${esc(actingPlayer.name)}'S ACTION</p><h2>${instruction()}</h2>
        <div class="prime-display">${centerDisplay(validation)}</div>
        ${actionControls(validation)}
        <h3>ゲームログ</h3><div class="log">${game.logs.map((entry) => `<div>${esc(entry)}</div>`).join("")}</div>
        <div class="footer-actions"><button type="button" class="btn danger" id="restart">ルーム退出</button></div>
      </section>
      ${playerPanel(game.players[1], 1)}
    </div>
  </div>`;
  bindGameControls();
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
    return `<div><div class="prime-number">${game.attack.value}</div><div class="equation">基本 ${game.attack.base} + B効果 ${game.attack.bCount}</div></div>`;
  }
  if (game.lastResult) {
    return `<div><div class="prime-number">${game.lastResult.damage}</div><div class="equation">攻撃 ${game.lastResult.attack} − 防御 ${game.lastResult.defense}</div></div>`;
  }
  return "";
}

function actionControls(validation) {
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
    return `<div class="actions"><button type="button" class="btn primary" id="defend-hand" ${selected.length < 1 || selected.length > 2 ? "disabled" : ""}>手札で防御</button><button type="button" class="btn gold" id="defend-ext" ${!game.external.length ? "disabled" : ""}>外部ゾーン</button></div><p class="hint">手札防御は1〜2枚の数字を合計します。素数である必要はありません。外部ゾーンは一番上の1枚を使います。</p>`;
  }
  if (game.phase === PHASES.RESULT) {
    return `<div class="actions"><button type="button" class="btn primary" id="next">次のターンへ</button></div><p class="hint">ライフ移動: ${game.lastResult.moved.join(", ") || "なし"} ／ Aドロー: ${game.lastResult.drawn}枚</p>`;
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
    resolveDefense(game, "hand", cards);
    selected = [];
    render();
    sync();
  });
  document.querySelector("#defend-ext")?.addEventListener("click", () => {
    resolveDefense(game, "external");
    selected = [];
    render();
    sync();
  });
  document.querySelector("#next")?.addEventListener("click", () => {
    endTurn(game);
    handoff(render);
  });
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

function passDrawModal() {
  const player = game.players[game.active];
  const choices = player.deck.slice(0, 2);
  if (!choices.length) {
    player.hasPassDrawBonus = false;
    render();
    sync();
    return;
  }
  if (choices.length === 1) {
    applyPassDraw(game, choices[0].id);
    render();
    sync();
    return;
  }
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal">
    <p class="eyebrow">PASS DRAW BONUS</p><h2>山札の上から1枚を選ぶ</h2>
    <p class="sub">選ばなかったカードは山札の一番下へ移動します。</p>
    <div class="choice-cards">${choices.map((card) => cardHtml(card)).join("")}</div>
  </div></div>`;
  modalRoot.querySelectorAll("[data-card]").forEach((element) => {
    element.addEventListener("click", () => {
      applyPassDraw(game, element.dataset.card);
      modalRoot.innerHTML = "";
      render();
      sync();
    });
  });
}

function gameOver() {
  clearSave();
  const winner = game.players[game.winner];
  modalRoot.innerHTML = `<div class="modal-backdrop"><div class="modal">
    <p class="eyebrow">GAME OVER</p><h1>${esc(winner.name)}<br>WIN</h1>
    <p class="sub">相手のライフゾーンをすべて攻略しました。</p>
    <button type="button" class="btn primary" id="back-title">タイトルへ</button>
  </div></div>`;
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
  if (event.key === "Escape" && modalRoot.children.length) modalRoot.innerHTML = "";
});

window.addEventListener("beforeunload", leaveNetwork);
title();
