using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace LeiKaiFeng.Http
{
    public sealed class MHttpClientHandler
    {
        internal static void LinkedTimeOutAndCancel(TimeSpan timeOutSpan, CancellationToken token, Action cancelAction, out CancellationToken outToken, out Action closeAction)
        {


            if (timeOutSpan == MHttpClientHandler.NeverTimeOutTimeSpan)
            {
                if (token == CancellationToken.None)
                {
                    outToken = token;

                    closeAction = () => { };
                }
                else
                {
                    outToken = token;

                    var register = outToken.Register(cancelAction);

                    closeAction = () => register.Dispose();
                }
            }
            else
            {
                if (token == CancellationToken.None)
                {
                    var source = new CancellationTokenSource(timeOutSpan);

                    outToken = source.Token;

                    var resgister = outToken.Register(cancelAction);

                    closeAction = () =>
                    {
                        resgister.Dispose();

                        source.Dispose();
                    };
                }
                else
                {
                    var source = new CancellationTokenSource(timeOutSpan);


                    var register_0 = token.Register(source.Cancel);

                    outToken = source.Token;

                    var register_1 = outToken.Register(cancelAction);

                    closeAction = () =>
                    {
                        register_1.Dispose();

                        register_0.Dispose();

                        source.Dispose();
                    };

                }
            }
        }

        public static Task<TR> TimeOutAndCancelAsync<T, TR>(Task<T> task, Func<T, TR> translateFunc, Action cancelAction, Func<Exception, bool> isCancelFunc, TimeSpan timeOutSpan, CancellationToken token)
        {
            LinkedTimeOutAndCancel(timeOutSpan, token, cancelAction, out var outToken, out var closeAction);

            async Task<TR> func()
            {
                T v;
                try
                {
                    try
                    {
                        v = await task.ConfigureAwait(false);
                    }
                    finally
                    {
                        closeAction();
                    }     
                }
                catch (Exception e)
                {
                    if (isCancelFunc(e))
                    {
                        throw new OperationCanceledException(string.Empty, e);
                    }
                    else
                    {
                        throw;
                    }
                }

                return translateFunc(v);
            }


            return func();
        }


        

        public static Func<Uri, MHttpClientHandler, CancellationToken, Task<MHttpStream>> CreateNewConnectAsync(Func<Socket, Uri, Task> connectFunc, Func<NetworkStream, Uri, Task<Stream>> authenticateFunc)
        {
            return (uri, handler, token) =>
            {
                Socket socket = new Socket(handler.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                async Task<Stream> func()
                {
                    await connectFunc(socket, uri).ConfigureAwait(false);

                    return await authenticateFunc(new NetworkStream(socket, true), uri).ConfigureAwait(false);
                }

                bool isCancel(Exception e)
                {
                    if (e is ObjectDisposedException ||
                        (e is IOException && e.InnerException is ObjectDisposedException))
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }

                return MHttpClientHandler.TimeOutAndCancelAsync(
                    func(),
                    (stream) => new MHttpStream(socket, stream),
                    socket.Close,
                    isCancel,
                    handler.ConnectTimeOut,
                    token);
            };
        }


        public static Func<Stream, Uri, Task<Stream>> CreateCreateAuthenticateAsyncFunc(string host, bool isCheckCertificate)
        {
            Func<Stream, SslStream> createSslStream;

            if (isCheckCertificate)
            {
                createSslStream = (stream) => new SslStream(stream, false);
            }
            else
            {
                createSslStream = (stream) => new SslStream(stream, false, (a, b, c, d) => true);
            }

            return async (stream, uri) =>
            {
                SslStream sslStream = createSslStream(stream);

                await sslStream.AuthenticateAsClientAsync(host).ConfigureAwait(false);


                return sslStream;
            };


        }

        public static Func<Socket, Uri, Task> CreateCreateConnectAsyncFunc(string host, int port)
        {
            return (socket, uri) => Task.Run(() => socket.Connect(host, port));
        }



        public static TimeSpan NeverTimeOutTimeSpan => TimeSpan.FromMilliseconds(-1);

        public Func<Uri, MHttpClientHandler, CancellationToken, Task<MHttpStream>> StreamCallback { get; set; }

        public AddressFamily AddressFamily { get; set; }

        public int MaxResponseContentSize { get; set; }

        public int MaxStreamPoolCount { get; set; }

        public int MaxStreamParallelRequestCount { get; set; }

     
        public int MaxStreamRequestCount { get; set; }

        public TimeSpan MaxStreamWaitTimeSpan { get; set; }

        public TimeSpan ConnectTimeOut { get; set; }

        public TimeSpan ResponseTimeOut { get; set; }


        public MHttpClientHandler()
        {
            MaxResponseContentSize = 1024 * 1024 * 10;

            MaxStreamPoolCount = 6;

            MaxStreamParallelRequestCount = 6;

            MaxStreamRequestCount = 6;

            AddressFamily = AddressFamily.InterNetwork;

            StreamCallback = CreateNewConnectAsync(CreateConnectAsync, CreateAuthenticateAsync);

            ResponseTimeOut = NeverTimeOutTimeSpan;

            ConnectTimeOut = NeverTimeOutTimeSpan;

            MaxStreamWaitTimeSpan = NeverTimeOutTimeSpan;
        }

        static Task CreateConnectAsync(Socket socket, Uri uri)
        {

            return socket.ConnectAsync(uri.Host, uri.Port);

        }


        static Task<Stream> CreateHttp(Stream stream, Uri uri)
        {
            return Task.FromResult(stream);
        }

        static async Task<Stream> CreateHttps(Stream stream, Uri uri)
        {
            
            SslStream sslStream = new SslStream(stream, false);

            await sslStream.AuthenticateAsClientAsync(uri.Host).ConfigureAwait(false);

            return sslStream;
        }

        static Task<Stream> CreateAuthenticateAsync(Stream stream, Uri uri)
        {
            if (uri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase))
            {
                return CreateHttp(stream, uri);
            }
            else if (uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
            {
                return CreateHttps(stream, uri);
            }
            else
            {
                throw new ArgumentException("Uri Scheme");
            }
        }
    }
}