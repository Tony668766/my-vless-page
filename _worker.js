import { connect } from 'cloudflare:sockets';
// ===================================================
// VLESS over WebSocket for Cloudflare Pages / Workers
// ===================================================

// 1. 請在此處更改您的 UUID（或使用預設值）
const userID = 'd3b07384-d113-424a-a112-d023147dceec';

// 2. 備用 ProxyIP（可留空，若邊緣網路被阻擋可填入第三方轉發 IP）
const proxyIP = '154.213.16.10';

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      
      // 如果不是 WebSocket 請求，回傳普通網頁（偽裝頁面）
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        const url = new URL(request.url);
        if (url.pathname === '/') {
          return new Response('Hello World', { status: 200 });
        }
        return new Response('Not Found', { status: 404 });
      }

      // 處理 VLESS WebSocket 握手
      return await vlessOverWSHandler(request);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  server.accept();

  let remoteSocketWrapper = {
    value: null,
  };

  let isDns = false;

  // 處理來自客戶端的數據流
  const readableWebSocketStream = makeReadableWebSocketStream(server);

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (remoteSocketWrapper.value) {
        const writer = remoteSocketWrapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      // 解析 VLESS 標頭
      const {
        hasError,
        message,
        portRemote,
        addressRemote,
        rawDataIndex,
        vlessVersion,
        isUDP
      } = processVlessHeader(chunk, userID);

      if (hasError) {
        controller.error(message);
        return;
      }

      // 建立 TCP 連線到目標伺服器
      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isUDP) {
        if (portRemote === 53) {
          isDns = true;
        } else {
          controller.error('UDP only supports DNS port 53');
          return;
        }
      }

      handleTCPOutBound(remoteSocketWrapper, addressRemote, portRemote, rawClientData, server, vlessResponseHeader, isDns);
    },
    close() {
      console.log('readableWebSocketStream closed');
    },
    abort(reason) {
      console.error('readableWebSocketStream aborted', reason);
    },
  })).catch((err) => {
    console.error('pipeTo error', err);
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function handleTCPOutBound(remoteSocketWrapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, isDns) {
  async function connectAndWrite(address, port) {
    const tcpSocket = connect({
      hostname: address,
      port: port,
    });
    remoteSocketWrapper.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote);

  // 接收遠端回應並傳回客戶端
  remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, isDns);
}

function makeReadableWebSocketStream(webSocketServer) {
  let stream = new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        const message = event.data;
        controller.enqueue(message);
      });
      webSocketServer.addEventListener('close', () => {
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        controller.error(err);
      });
    },
  });
  return stream;
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data length' };
  }

  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = true;

  // UUID 驗證邏輯
  const slicedUuid = new Uint8Array(vlessBuffer.slice(1, 17));
  const hexUuid = Array.from(slicedUuid).map(b => b.toString(16).padStart(2, '0')).join('');
  const formattedUuid = `${hexUuid.slice(0, 8)}-${hexUuid.slice(8, 12)}-${hexUuid.slice(12, 16)}-${hexUuid.slice(16, 20)}-${hexUuid.slice(20)}`;

  if (formattedUuid !== userID) {
    isValidUser = false;
  }

  if (!isValidUser) {
    return { hasError: true, message: 'invalid user' };
  }

  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 19 + optLength))[0];

  const isUDP = command === 2;
  const portIndex = 19 + optLength;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);

  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));
  const addressType = addressBuffer[0];

  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressRemote = '';

  switch (addressType) {
    case 1:
      addressLength = 4;
      addressRemote = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
      break;
    case 2:
      addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressRemote = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3:
      addressLength = 16;
      const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressRemote = ipv6.join(':');
      break;
    default:
      return { hasError: true, message: `invaild addressType: ${addressType}` };
  }

  const rawDataIndex = addressValueIndex + addressLength;

  return {
    hasError: false,
    addressRemote,
    portRemote,
    rawDataIndex,
    vlessVersion: version,
    isUDP,
  };
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, isDns) {
  let vlessHeaderSent = false;
  await remoteSocket.readable.pipeTo(
    new WritableStream({
      start() {},
      async write(chunk, controller) {
        if (webSocket.readyState !== 1) {
          controller.error('webSocket is not open');
          return;
        }
        if (vlessHeaderSent) {
          webSocket.send(chunk);
        } else {
          const newChunk = new Uint8Array(vlessResponseHeader.byteLength + chunk.byteLength);
          newChunk.set(vlessResponseHeader, 0);
          newChunk.set(new Uint8Array(chunk), vlessResponseHeader.byteLength);
          webSocket.send(newChunk);
          vlessHeaderSent = true;
        }
      },
      close() {
        console.log('remoteSocket.readable is closed');
      },
      abort(reason) {
        console.error('remoteSocket.readable abort', reason);
      },
    })
  ).catch((err) => {
    console.error('remoteSocketToWS error:', err);
  });
}
