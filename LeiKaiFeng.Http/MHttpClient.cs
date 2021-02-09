using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Channels;

namespace LeiKaiFeng.Http
{
    [Serializable]
    public sealed class MHttpResponseException : Exception
    {
        public int Status { get; private set; }

        public MHttpResponseException(int status)
        {
            Status = status;
        }
    }


    [Serializable]
    public sealed class MHttpClientException : Exception
    {
        public MHttpClientException(Exception e) : base(string.Empty, e)
        {

        }
    }

    sealed class MHttpStreamPack
    {
        sealed class ResponsePack
        {
            readonly TaskCompletionSource<MHttpResponse> m_source = new TaskCompletionSource<MHttpResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

            public ResponsePack(CancellationToken token, TimeSpan timeSpan, int maxResponseSize)
            {
                Token = token;
            
                TimeSpan = timeSpan;
                
                MaxResponseSize = maxResponseSize;
            }

            public CancellationToken Token { get; private set; }

            public TimeSpan TimeSpan { get; private set; }

            public int MaxResponseSize { get; private set; }

            public Task<MHttpResponse> Task => m_source.Task;

            public void Send(Exception e)
            {
                m_source.TrySetException(e);
            }

            public void Send(MHttpResponse response)
            {
                m_source.TrySetResult(response);
            }
        }

        sealed class Pack
        {
            readonly object m_lock = new object();

            
            int m_refCount = 1;

            int m_is_close = 0;

            MHttpStream m_stream;


            public Pack(MHttpStream stream)
            {
                m_stream = stream;
            }


            public void Add()
            {
                bool isThrow = false;


                lock (m_lock)
                {
                    if (m_refCount == 0)
                    {
                        isThrow = true;
                    }
                    else
                    {
                        m_refCount++;

                        isThrow = false;
                    }
                }

                if (isThrow)
                {
                    
                    throw new ObjectDisposedException(nameof(MHttpStream));
                }
            }

            public void Sub()
            {
                bool b;

                lock (m_lock)
                {
                    if (m_refCount == 0)
                    {
                        b = false;
                    }
                    else
                    {
                        m_refCount--;

                        if (m_refCount == 0)
                        {
                            b = true;
                        }
                        else
                        {
                            b = false;
                        }
                    }
                }

                if (b)
                {
                    Console.WriteLine("run");

                    m_stream.Close();
                }
            }

            public void Close()
            {
                if (Interlocked.Exchange(ref m_is_close, 1) == 0)
                {
                    Sub();
                }
            }

            public Task WriteAsync(Func<MHttpStream, Task> func)
            {
                return func(m_stream);
            }


            public Task<MHttpResponse> ReadAsync(int maxResponseSize)
            {
                return MHttpResponse.ReadAsync(m_stream, maxResponseSize);
            }

            public void Cancel()
            {
                m_stream.Cencel();
            }



        }

        readonly ChannelReader<ResponsePack> m_channelReader;

        readonly ChannelWriter<ResponsePack> m_channelWriter;

        readonly Pack m_pack;

        int m_count;

        public MHttpStreamPack(MHttpStream stream, int maxRequestCount)
        {
            m_pack = new Pack(stream);

            var channel = Channel.CreateBounded<ResponsePack>(maxRequestCount);

            m_channelReader = channel;

            m_channelWriter = channel;

            m_count = 0;
        }

        public Task<MHttpResponse> SendAsync(Func<MHttpStream, Task> sendRequestFunc, Action readOverAction, TimeSpan timeSpan, CancellationToken token, int maxResponseSize)
        {
            return SendAsync(sendRequestFunc, readOverAction, new ResponsePack(token, timeSpan, maxResponseSize));
        }

        

        Task<MHttpResponse> SendAsync(Func<MHttpStream, Task> sendRequestFunc, Action readOverAction, ResponsePack taskPack)
        {
            async Task func()
            {
                try
                {
                    try
                    {
                        m_pack.Add();

                        await m_pack.WriteAsync(sendRequestFunc).ConfigureAwait(false);

                    }
                    finally
                    {
                        m_pack.Sub();
                    }

                   
                    await m_channelWriter.WriteAsync(taskPack).ConfigureAwait(false);

                    ReadResponse();



                    readOverAction();
                }
                catch(Exception e)
                {
                    taskPack.Send(e);
                }         
            }


            func();

            return taskPack.Task;
        }

        public void Close()
        {
            m_pack.Close();
        }

        void ReadResponse()
        {
            if (Interlocked.Increment(ref m_count) == 1)
            {
                ThreadPool.QueueUserWorkItem((obj) => ReadResponseAsync());
            }
        }

        

        //这个地方的主要功能在于让读取一个一个的进行,不能并行读取
        async Task ReadResponseAsync()
        {
            
            do
            {
                if (!m_channelReader.TryRead(out var taskPack))
                {
                    throw new NotImplementedException("内部错误");
                }
                else
                {

                    
                    MHttpClient.LinkedTimeOutAndCancel(taskPack.TimeSpan, taskPack.Token, m_pack.Cancel, out var token, out var closeAction);

                    Action action;

                    try
                    {
                        MHttpResponse response;

                        try
                        {
                            m_pack.Add();

                            response = await m_pack.ReadAsync(taskPack.MaxResponseSize).ConfigureAwait(false);

                        }
                        finally
                        {
                            m_pack.Sub();
                        }

                        
                        action = () => taskPack.Send(response);
                    }
                    catch(Exception e)
                    {
                        if (token.IsCancellationRequested)
                        {
                            action = () => taskPack.Send(new OperationCanceledException(string.Empty, e));
                        }
                        else
                        {
                            action = () => taskPack.Send(e);
                        }
                    }

                    closeAction();

                    action();
                }
            }
            while (Interlocked.Decrement(ref m_count) != 0);
        }
    }

    public sealed class MHttpClient
    {
        internal static void LinkedTimeOutAndCancel(TimeSpan timeOutSpan, CancellationToken token, Action cancelAction, out CancellationToken outToken, out Action closeAction)
        {


            if (timeOutSpan == MHttpClient.NeverTimeOutTimeSpan)
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

        public static Task<TR> TimeOutAndCancelAsync<T, TR>(Task<T> task, Func<T, TR> translateFunc, Action cancelAction,TimeSpan timeOutSpan, CancellationToken token)
        {
            LinkedTimeOutAndCancel(timeOutSpan, token, cancelAction, out var outToken, out var closeAction);
            
            async Task<TR> func()
            {
                T v;
                try
                {
                    v = await task.ConfigureAwait(false);     
                }
                catch(Exception e)
                {
                    if (outToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(string.Empty, e);
                    }
                    else
                    {
                        throw;
                    }
                }
                finally
                {
                    closeAction();
                }

                return translateFunc(v);
            }


            return func();
        }



        public static TimeSpan NeverTimeOutTimeSpan => TimeSpan.FromMilliseconds(-1);



        readonly StreamPool m_pool;

        readonly MHttpClientHandler m_handler;

        public TimeSpan ConnectTimeOut { get; set; }

        public TimeSpan ResponseTimeOut { get; set; }

        

        public MHttpClient() : this(new MHttpClientHandler())
        {

        }


        public MHttpClient(MHttpClientHandler handler)
        {
            m_handler = handler;

            m_pool = new StreamPool(handler.MaxStreamPoolCount);

            ResponseTimeOut = NeverTimeOutTimeSpan;

            ConnectTimeOut = NeverTimeOutTimeSpan;
        }

        async Task<Stream> CreateNewConnectAsync(Socket socket, Uri uri)
        {
            await m_handler.ConnectCallback(socket, uri).ConfigureAwait(false);

            return await m_handler.AuthenticateCallback(new NetworkStream(socket, true), uri).ConfigureAwait(false);
        }

        Task<MHttpStreamPack> CreateNewConnectAsync(Uri uri, CancellationToken cancellationToken)
        {
            Socket socket = new Socket(m_handler.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            return TimeOutAndCancelAsync(
                CreateNewConnectAsync(socket, uri),
                (stream) => new MHttpStreamPack(new MHttpStream(socket, stream), m_handler.MaxOneStreamRequestCount),
                socket.Close,
                ConnectTimeOut,
                cancellationToken);
        }


        async Task<T> Internal_SendAsync<T>(Uri uri, MHttpRequest request, CancellationToken cancellationToken, Func<MHttpResponse, T> translateFunc)
        {
            try
            {
                MHttpStreamPack pack = default;

                var requsetFunc = request.CreateSendAsync();

                bool b = false;

                void setPool() => b = m_pool.Set(uri, pack);

                Task<MHttpResponse> func() => pack.SendAsync(requsetFunc, setPool, ResponseTimeOut, cancellationToken, m_handler.MaxResponseSize);

                while (m_pool.Get(uri, out pack))
                {
                  
                    try
                    {
                        var v = translateFunc(await func().ConfigureAwait(false));

                        if (b == false)
                        {
                            pack.Close();
                        }

                        return v;
                    }
                    catch (Exception e)
                    {
                        pack.Close();

                        if (e is IOException ||
                            e is ObjectDisposedException)
                        {

                        }
                        else
                        {
                            throw;
                        }
                    }
                    
                }

                try
                {
                    pack = await CreateNewConnectAsync(uri, cancellationToken).ConfigureAwait(false);


                    var v = translateFunc(await func().ConfigureAwait(false));

                    if (b == false)
                    {
                        pack.Close();
                    }

                    return v;
                }
                catch
                {
                    pack.Close();

                    throw;
                }

                

            }
            catch(Exception e)
            {
                throw new MHttpClientException(e);
            }     
        }

        public Task<MHttpResponse> SendAsync(Uri uri, MHttpRequest request, CancellationToken cancellationToken)
        {
            return Internal_SendAsync(uri, request, cancellationToken, (res) => res);
        }


        static void ChuckResponseStatus(MHttpResponse response)
        {
            int n = response.Status;

            if (n >= 200 && n < 300)
            {

            }
            else
            {
                throw new MHttpResponseException(n);
            }
        }

        public Task<string> GetStringAsync(Uri uri, CancellationToken cancellationToken)
        {
            MHttpRequest request = MHttpRequest.CreateGet(uri);

            return Internal_SendAsync(uri, request, cancellationToken, (response) =>
            {
                ChuckResponseStatus(response);


                return response.Content.GetString();
            });
            
        }

        public Task<byte[]> GetByteArrayAsync(Uri uri, Uri referer, CancellationToken cancellationToken)
        {

            MHttpRequest request = MHttpRequest.CreateGet(uri);

            request.Headers.Set("Referer", referer.AbsoluteUri);

            return Internal_SendAsync(uri, request, cancellationToken, (response) =>
            {



                ChuckResponseStatus(response);

                return response.Content.GetByteArray();
            });

        }

        public Task<byte[]> GetByteArrayAsync(Uri uri, CancellationToken cancellationToken)
        {
            MHttpRequest request = MHttpRequest.CreateGet(uri);

            return Internal_SendAsync(uri, request, cancellationToken, (response) =>
            {
                ChuckResponseStatus(response);


                return response.Content.GetByteArray();
            });

        }


    }
}