/*
 * Author: ByronP
 * Date: 1/14/2017
 * Mod: 01/30/2019
 * Coinigy Inc. Coinigy.com
 */
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PureWebSockets
{
    public class PureWebSocket : IDisposable
    {
        private string Url { get; }
        private ClientWebSocket _ws;
        private readonly PureWebSocketOptions _options;
        private readonly BlockingCollection<KeyValuePair<DateTime, string>> _sendQueue = new BlockingCollection<KeyValuePair<DateTime, string>>();
        private bool _reconnecting, _disconnectCalled, _listenerRunning, _senderRunning;
        private CancellationTokenSource _tokenSource = new CancellationTokenSource();
        private Task _listenerTask;
        private Task _senderTask;

        /// <summary>
        /// The current state of the connection.
        /// </summary>
        public WebSocketState State => _ws.State;
        /// <summary>
        /// The current number of items waiting to be sent.
        /// </summary>
        public int SendQueueLength => _sendQueue.Count;

        public event Data OnData;
        public event Message OnMessage;
        public event Opened OnOpened;
        public event Error OnError;
        public event SendFailed OnSendFailed;
        public bool RunMonitor = true;
        public bool MonitorFinished = false;

        public PureWebSocket(string url, IPureWebSocketOptions options)
        {
            _options = (PureWebSocketOptions)options;
            Url = url;

            Log("Creating new instance.");

            Task.Run(() =>
            {
                Monitor();
            });
        }
        public void Dispose()
        {
            //Console.WriteLine(_ws.State);
            if(_ws.State > WebSocketState.Connecting && _ws.State < WebSocketState.CloseReceived)
                _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "CLIENT REQUESTED CLOSE", _tokenSource.Token);
        }
        private void InitializeClient()
        {
            _ws = new ClientWebSocket();

            try
            {
                if (_options.IgnoreCertErrors)
                {
                    //NOTE: this will not work and a workaround will be available in netstandard 2.1
                    ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, errors) => true;
                }
            }
            catch (Exception ex)
            {
                Log($"Setting invalid certificate options threw an exception: {ex.Message}. Defaulting IgnoreCertErrors to false.");
                _options.IgnoreCertErrors = false;
            }

            if(_options.Cookies != null && _options.Cookies.Count > 0)
                _ws.Options.Cookies = _options.Cookies;

            if(_options.ClientCertificates != null && _options.ClientCertificates.Count > 0)
                _ws.Options.ClientCertificates = _options.ClientCertificates;

            if (_options.Proxy != null)
                _ws.Options.Proxy = _options.Proxy;

            if (_options.SubProtocols != null)
            {
                foreach (var protocol in _options.SubProtocols)
                {
                    try
                    {
                        _ws.Options.AddSubProtocol(protocol);
                    }
                    catch (Exception ex)
                    {
                        Log("Invalid or unsupported sub protocol, value: " + protocol + ", exception: " + ex.Message, nameof(_options.SubProtocols));
                    }
                }
            }

            // optionally add request header e.g. X-Key, testapikey123
            if (_options.Headers != null)
                foreach (var h in _options.Headers)
                {
                    try
                    {
                        _ws.Options.SetRequestHeader(h.Item1, h.Item2);
                    }
                    catch (Exception ex)
                    {
                        Log("Invalid or unsupported header, value: " + h + ", exception: " + ex.Message, nameof(_options.Headers));
                    }
                }
        }
        public bool Send(string data)
        {
            try
            {
                if ((State != WebSocketState.Open && !_reconnecting) || SendQueueLength >= _options.SendQueueLimit)
                {
                    Log(SendQueueLength >= _options.SendQueueLimit ? $"Could not add item to send queue: queue limit reached, Data {data}" : $"Could not add item to send queue: State {State}, Queue Count {SendQueueLength}, Data {data}");
                    return false;
                }

                Task.Run(() =>
                {
                    Log($"Adding item to send queue: Data {data}");
                    _sendQueue.Add(new KeyValuePair<DateTime, string>(DateTime.UtcNow, data));
                }).Wait(100, _tokenSource.Token);

                return true;
            }
            catch (Exception ex)
            {
                Log($"Send threw exception: {ex.Message}.");
                OnError?.Invoke(ex);
                throw;
            }
        }
        private void Monitor()
        {
            Log($"Starting monitor.");
            _tokenSource = new CancellationTokenSource();
            while (RunMonitor)
            {
                do
                {
                    InitializeClient();

                    Uri url = new Uri(Url);
                    try
                    {
                        _reconnecting = true;

                        if (_disconnectCalled == true)
                        {
                            _disconnectCalled = false;
                            Log("Re-connecting closed connection.");
                        }

                        _ws.ConnectAsync(url, _tokenSource.Token).Wait(15000);
                    }
                    catch (Exception e)
                    {
                        Log(e.Message);
                        _ws.Abort();
                        _ws.Dispose();
                        Thread.Sleep(_options.MyReconnectStrategy.GetReconnectInterval() - 1000);
                    }
                } while (RunMonitor && _ws.State != WebSocketState.Open);
                _reconnecting = false;
                Console.WriteLine("Starting Listener & Sender tasks.");
                StartListener();
                StartSender();
                OnOpened?.Invoke();
                while (_ws.State == WebSocketState.Open)
                {
                    Thread.Sleep(200);
                }
                _reconnecting = true;
                for (int i=0;i<10;i++)
                {
                    if((_listenerRunning || _senderRunning) == false)
                    {
                        break;
                    } else
                    {
                        Thread.Sleep(1000);
                    }

                }
            }
            MonitorFinished = true;
        }
        private void StartListener()
        {
            Log($"Starting listener. {_ws.State}");
            _listenerTask = Task.Run(async () =>
            {
                Log("Entering listener loop.");
                _listenerRunning = true;
                try
                {
                    while (_ws.State == WebSocketState.Open && !_reconnecting)
                    {
                        var message = "";
                        var binary = new List<byte>();

                    READ:

                        var buffer = new byte[1024];
                        WebSocketReceiveResult res;

                        try
                        {
                            res = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), _tokenSource.Token);
                        }
                        catch (Exception ex)
                        {
                            Log($"Receive threw an exception: {ex.Message}");
                            // Most likely socket error
                            _ws.Abort();
                            break;
                        }

                        if (res == null)
                            goto READ;

                        if (res.MessageType == WebSocketMessageType.Close)
                        {
                            Log("Server requested close.");
                            //await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "SERVER REQUESTED CLOSE", _tokenSource.Token);
                            _disconnectCalled = true;
                            break;
                        }

                        // handle text data
                        if (res.MessageType == WebSocketMessageType.Text)
                        {
                            if (!res.EndOfMessage)
                            {
                                message += Encoding.UTF8.GetString(buffer).TrimEnd('\0');
                                goto READ;
                            }
                            message += Encoding.UTF8.GetString(buffer).TrimEnd('\0');

                            // support ping/pong if initiated by the server (see RFC 6455)
                            if (message.Trim() == "ping")
                                _ = Send("pong");
                            else
                            {
                                Log($"Message fully received: {message}");
                                Task.Run(() => OnMessage?.Invoke(message)).Wait(50);
                            }
                        }
                        else
                        {
                            var exactDataBuffer = new byte[res.Count];
                            Array.Copy(buffer, 0, exactDataBuffer, 0, res.Count);
                            // handle binary data
                            if (!res.EndOfMessage)
                            {
                                binary.AddRange(exactDataBuffer);
                                goto READ;
                            }

                            binary.AddRange(exactDataBuffer);
                            var binaryData = binary.ToArray();
                            LogData("Binary fully received", binaryData);
                            Task.Run(() => OnData?.Invoke(binaryData)).Wait(50);
                        }

                        // ReSharper disable once RedundantAssignment
                        buffer = null;
                    }
                }
                catch (Exception ex)
                {
                    Log($"Listener threw exception: {ex.Message}.");
                    OnError?.Invoke(ex);
                }
                _listenerRunning = false;
                Log($"Listener exiting { _ws.State}");
                return Task.CompletedTask;
            });
        }
        private void StartSender()
        {
            Log($"Starting sender. {_ws.State}");
            _senderTask = Task.Run(async () =>
            {
                Log("Entering sender loop.");
                _senderRunning = true;
                try
                {
                    while (!_reconnecting && _ws.State != WebSocketState.Aborted)
                    {
                        if (_ws.State == WebSocketState.Open && !_reconnecting)
                        {
                            if (SendQueueLength == 0) continue;
                            var msg = _sendQueue.Take(_tokenSource.Token);
                            if (msg.Key.Add(_options.SendCacheItemTimeout) < DateTime.UtcNow)
                            {
                                Log($"Message expired skipping: {msg.Key} {msg.Value}.");
                                continue;
                            }
                            var buffer = Encoding.UTF8.GetBytes(msg.Value);
                            try
                            {
                                Log($"Sending message: {msg.Key} {msg.Value}.");
                                await _ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, _tokenSource.Token).ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                Log($"Sender threw sending exception: {ex.Message}.");
                                // Most likely socket error
                                OnSendFailed?.Invoke(msg.Value, ex);
                                _ws.Abort();
                                break;
                            }
                        }
                        // limit to N ms per iteration
                        Thread.Sleep(_options.SendDelay);
                    }
                }
                catch (Exception ex)
                {
                    Log($"Sender threw exception: {ex.Message}.");
                    OnSendFailed?.Invoke("", ex);
                    OnError?.Invoke(ex);
                }
                _senderRunning = false;
                Log($"Exiting sender. { _ws.State}");
                return Task.CompletedTask;
            });
        }
        internal void Log(string message, [CallerMemberName] string memberName = "",// [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
        {
            if (_options.DebugMode)
                Task.Run(() => Console.WriteLine($"{DateTime.Now:O} {lineNumber} PureWebSocket.{memberName}: {message}"));
        }
        internal void LogData(string message, byte[] data, [CallerMemberName] string memberName = "")
        {
            if (_options.DebugMode)
                Task.Run(() => _options.DebugOutput.WriteLine($"{DateTime.Now:O} PureWebSocket.{memberName}: {message}, data: {BitConverter.ToString(data)}"));
        }
    }
}