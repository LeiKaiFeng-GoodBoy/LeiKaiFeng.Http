using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Channels;
using System.Collections.Generic;

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

    sealed class RequestAndResponsePack
    {
        readonly TaskCompletionSource<MHttpResponse> m_source = new TaskCompletionSource<MHttpResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        public RequestAndResponsePack(CancellationToken token, TimeSpan timeSpan, int maxResponseContentSize, Func<MHttpStream, Task> writeRequest)
        {
            Token = token;
          
            TimeSpan = timeSpan;
            
            MaxResponseContentSize = maxResponseContentSize;
            
            WriteRequest = writeRequest;
        }

        public CancellationToken Token { get; private set; }

        public TimeSpan TimeSpan { get; private set; }

        public int MaxResponseContentSize { get; private set; }

        public Func<MHttpStream, Task> WriteRequest { get; set; }


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

    sealed class RequestAndResponse
    {
        

        static async Task ReadResponseAsync(ChannelReader<RequestAndResponsePack> reader, MHttpStream stream)
        {
            try
            {
                //需要read端出现异常,task清空后才会推出
                while (true)
                {
                    var pack = await reader.ReadAsync().ConfigureAwait(false);



                    MHttpClient.LinkedTimeOutAndCancel(pack.TimeSpan, pack.Token, stream.Cencel, out var token, out var closeAction);

                    try
                    {
                        MHttpResponse response = await MHttpResponse.ReadAsync(stream, pack.MaxResponseContentSize).ConfigureAwait(false);

                        pack.Send(response);

                    }
                    catch(IOException e)
                    when(e.InnerException is ObjectDisposedException)
                    {
                        pack.Send(new OperationCanceledException());
                    }
                    catch(ObjectDisposedException)
                    {
                        pack.Send(new OperationCanceledException());
                    }
                    catch(Exception e)
                    {
                        pack.Send(e);
                    }
                    finally
                    {
                        closeAction();
                    }
                    

                }
            }
            catch (ChannelClosedException)
            {

            }
        }


        static async Task WriteRequestAsync(ChannelReader<RequestAndResponsePack> reader, ChannelWriter<RequestAndResponsePack> writer, MHttpStream stream)
        {
            try
            {
                while (true)
                {
                    var pack = await reader.ReadAsync().ConfigureAwait(false);

                    try
                    {
                        await pack.WriteRequest(stream).ConfigureAwait(false);

                    }
                    catch(Exception e)
                    {
                        pack.Send(e);

                        writer.TryComplete();

                        return;
                    }

                    await writer.WriteAsync(pack).ConfigureAwait(false);
                }
            }
            catch (ChannelClosedException)
            {
                writer.TryComplete();
            }
        }


        static Task AddTask2(int maxRequestCount, ChannelReader<RequestAndResponsePack> reader, MHttpStream stream)
        {
            var channel = Channel.CreateBounded<RequestAndResponsePack>(maxRequestCount);


            var read_task = ReadResponseAsync(channel, stream);

            var write_task = WriteRequestAsync(reader, channel, stream);


            return Task.WhenAll(read_task, write_task);
        }

        static async Task AddTask(int maxStreamPoolCount, int maxRequestCount, ChannelReader<RequestAndResponsePack> reader, Func<Task<MHttpStream>> func)
        {
            
            async Task<MHttpStream> createStream()
            {
                while (true)
                {
                    bool b = await reader.WaitToReadAsync().ConfigureAwait(false);

                    if (b == false) 
                    {
                        throw new ChannelClosedException();
                    }

                    Exception exception;
                    try
                    {
                        return await func().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }

                    var pack = await reader.ReadAsync().ConfigureAwait(false);

                    pack.Send(exception);

                }
            }

            try
            {
                SemaphoreSlim slim = new SemaphoreSlim(maxStreamPoolCount, maxStreamPoolCount);
              
                while (true)
                {

                    await slim.WaitAsync().ConfigureAwait(false);

                    MHttpStream stream = await createStream().ConfigureAwait(false);

                    Task task = Task.Run(() => AddTask2(maxRequestCount, reader, stream))
                        .ContinueWith((t) => {

                            slim.Release();

                            stream.Close();


                        });

                }

            }
            catch (ChannelClosedException)
            {

            }

        }

        public static ChannelWriter<RequestAndResponsePack> Create(int maxStreamPoolCount, int maxRequestCount, Func<Task<MHttpStream>> func)
        {
            var channel = Channel.CreateBounded<RequestAndResponsePack>(maxStreamPoolCount);

            Task.Run(() => AddTask(maxStreamPoolCount, maxRequestCount, channel, func));


            return channel;

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

            m_pool = new StreamPool();

            ResponseTimeOut = NeverTimeOutTimeSpan;

            ConnectTimeOut = NeverTimeOutTimeSpan;
        }

        async Task<Stream> CreateNewConnectAsync(Socket socket, Uri uri)
        {
            await m_handler.ConnectCallback(socket, uri).ConfigureAwait(false);

            return await m_handler.AuthenticateCallback(new NetworkStream(socket, true), uri).ConfigureAwait(false);
        }

        Task<MHttpStream> CreateNewConnectAsync(Uri uri, CancellationToken cancellationToken)
        {
            Socket socket = new Socket(m_handler.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            return TimeOutAndCancelAsync(
                CreateNewConnectAsync(socket, uri),
                (stream) => new MHttpStream(socket, stream),
                socket.Close,
                ConnectTimeOut,
                cancellationToken);
        }


        async Task<T> Internal_SendAsync<T>(Uri uri, MHttpRequest request, CancellationToken cancellationToken, Func<MHttpResponse, T> translateFunc)
        {
            try
            {
                
                var writer = m_pool.Find(
                        uri,
                        m_handler.MaxStreamPoolCount,
                        m_handler.MaxOneStreamRequestCount,
                        () => CreateNewConnectAsync(uri, cancellationToken));


               

                while (true)
                {
                    try
                    {
                        var pack = new RequestAndResponsePack(
                               cancellationToken,
                               ResponseTimeOut,
                               m_handler.MaxResponseSize,
                               request.CreateSendAsync());

                        await writer.WriteAsync(pack).ConfigureAwait(false);

                        var response = await pack.Task.ConfigureAwait(false);

                        return translateFunc(response);
                    }
                    catch (IOException)
                    {

                    }
                    catch (ObjectDisposedException)
                    {

                    }


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