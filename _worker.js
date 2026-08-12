import { connect } from 'cloudflare:sockets';

// ===================================================
// VLESS over WebSocket for Cloudflare Pages
// ===================================================

const userID = 'd3b07384-d113-424a-a112-d023147dceec';

// ⚠️ 必須填寫「純數字 IP」，切勿填寫域名！
// 這裡提供社群最穩定的反代 IP（若依然不行可替換為 8.219.193.30 或 103.200.112.108）
const proxyIP = '154.213.16.10';

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        const url = new URL(request.url);
        if (url.pathname === '/') {
          return new Response('Hello World', { status: 200 });
        }
        return new Response('Not Found', { status: 404 });
      }
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

  let remoteSocketWrapper = { value: null };
  let isDns = false;

  const readableWebSocketStream = makeReadableWebSocketStream(server);

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (remoteSocketWrapper.value) {
        const writer = remoteSocketWrapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

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
    close() {},
    abort(reason) {},
  })).catch((err) => {});

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

  let tcpSocket;
  // 如果設定了 ProxyIP，直接透過 ProxyIP 轉發出站，避免直連阻斷
  try {
    if (proxyIP) {
      tcpSocket = await connectAndWrite(proxyIP, portRemote);
    } else {
      tcpSocket = await connectAndWrite(addressRemote, portRemote);
    }
  } catch (err) {
    // 若代理連線失敗，備用直連
    tcpSocket = await connectAndWrite(addressRemote, portRemote);
  }

  remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, isDns);
}

function makeReadableWebSocketStream(webSocketServer) {
  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        controller.enqueue(event.data);
      });
      webSocketServer.addEventListener('close', () => {
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        controller.error(err);
      });
    },
  });
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data length' };
  }

  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  const slicedUuid = new Uint8Array(vlessBuffer.slice(1, 17));
  const hexUuid = Array.from(slicedUuid).map(b => b.toString(16).padStart(2, '0')).join('');
  const formattedUuid = `${hexUuid.slice(0, 8)}-${hexUuid.slice(8, 12)}-${hexUuid.slice(12, 16)}-${hexUuid.slice(16, 20)}-${hexUuid.slice(20)}`;

  if (formattedUuid !== userID) {
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
      close() {},
      abort(reason) {},
    })
  ).catch((err) => {});
}
