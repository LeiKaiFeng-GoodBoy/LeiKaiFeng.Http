using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Channels;
using System.Collections.Generic;
using System.Linq;

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

    public sealed class RequestPack
    {
        readonly TaskCompletionSource<MHttpResponse> m_source = new TaskCompletionSource<MHttpResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        public RequestPack(CancellationToken token, Func<MHttpStream, Task> writeRequest)
        {
            Token = token;
       
            WriteRequest = writeRequest;
        }

        public CancellationToken Token { get; private set; }

        public Func<MHttpStream, Task> WriteRequest { get; private set; }

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


    public sealed class MHttpStreamPack
    {
        

        static async Task ReadResponseAsync(MHttpClientHandler handler, ChannelReader<RequestPack> reader, MHttpStream stream)
        {
            //需要read端出现异常,task清空后才会推出
            while (true)
            {
                RequestPack pack;

                try
                {
                    pack = await reader.ReadAsync().ConfigureAwait(false);
                }
                catch (ChannelClosedException)
                {
                    return;
                }

                MHttpClientHandler.LinkedTimeOutAndCancel(handler.ResponseTimeOut, pack.Token, stream.Cencel, out var token, out var closeAction);
                try
                {
                    MHttpResponse response;
                    try
                    {
                        response = await MHttpResponse.ReadAsync(stream, handler.MaxResponseContentSize).ConfigureAwait(false);
                    }
                    finally
                    {
                        closeAction();
                    }
                    
                    pack.Send(response);

                }
                catch (IOException e)
                when (e.InnerException is ObjectDisposedException)
                {
                    pack.Send(new OperationCanceledException());
                }
                catch (ObjectDisposedException)
                {
                    pack.Send(new OperationCanceledException());
                }
                catch (Exception e)
                {
                    pack.Send(e);
                }
            }
        }


        static async Task WriteRequestAsync(MHttpClientHandler handler, ChannelReader<RequestPack> reader, ChannelWriter<RequestPack> writer, MHttpStream stream)
        {
            try
            {
                foreach (var item in Enumerable.Range(0, handler.MaxStreamRequestCount))
                {
                    RequestPack pack;

                    using (var cancel = new CancellationTokenSource(handler.MaxStreamWaitTimeSpan))
                    {
                        pack = await reader.ReadAsync(cancel.Token).ConfigureAwait(false);
                    }

                    try
                    {
                        await pack.WriteRequest(stream).ConfigureAwait(false);

                    }
                    catch(Exception e)
                    {
                        pack.Send(e);

                        return;
                    }

                    await writer.WriteAsync(pack).ConfigureAwait(false);
                }
            }
            catch (ChannelClosedException)
            {
                
            }
            catch (OperationCanceledException)
            {
                
            }
            finally
            {
                writer.TryComplete();
            }
        }


        static Task AddTask2(MHttpClientHandler handler, ChannelReader<RequestPack> reader, MHttpStream stream)
        {
            var options = new BoundedChannelOptions(handler.MaxStreamParallelRequestCount)
            {
                SingleReader = true,

                SingleWriter = true
            };

            var channel = Channel.CreateBounded<RequestPack>(options);

            return Task.WhenAll(
                Task.Run(() => ReadResponseAsync(handler, channel, stream)),
                Task.Run(() => WriteRequestAsync(handler, reader, channel, stream)));
        }

        static async Task AddTask1(MHttpClientHandler handler, Uri uri, ChannelReader<RequestPack> reader, SemaphoreSlim slim)
        {
            async Task<MHttpStream> createStream()
            {
                while (true)
                {
                    Exception exception;
                    try
                    {
                        return await handler.StreamCallback(uri, handler, CancellationToken.None).ConfigureAwait(false);
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
                MHttpStream stream;

                try
                {
                    stream = await createStream().ConfigureAwait(false);

                }
                catch (ChannelClosedException)
                {
                    return;
                }

                try
                {

                    await AddTask2(handler, reader, stream).ConfigureAwait(false);
                }
                finally
                {
                    stream.Close();
                }
            }
            finally
            {
                slim.Release();
            }
        }

        static async Task AddTask(MHttpClientHandler handler, Uri uri, ChannelReader<RequestPack> reader)
        {
            SemaphoreSlim slim = new SemaphoreSlim(handler.MaxStreamPoolCount, handler.MaxStreamPoolCount);

            while (true)
            {

                bool isHaveItem = await reader.WaitToReadAsync().ConfigureAwait(false);

                if (isHaveItem == false)
                {
                    return;
                }

                await slim.WaitAsync().ConfigureAwait(false);

                Task task = Task.Run(() => AddTask1(handler, uri, reader, slim));
            }
        }

        public static ChannelWriter<RequestPack> Create(MHttpClientHandler handler, Uri uri)
        {
            var options = new BoundedChannelOptions(handler.MaxStreamPoolCount)
            {

                AllowSynchronousContinuations = true,

                SingleWriter = true
            };

            var channel = Channel.CreateBounded<RequestPack>(options);

            Task.Run(() => AddTask(handler, uri, channel));


            return channel;

        }
    }

    public sealed class MHttpClient
    {
      


        readonly StreamPool m_pool;

        readonly MHttpClientHandler m_handler;


        

        public MHttpClient() : this(new MHttpClientHandler())
        {

        }


        public MHttpClient(MHttpClientHandler handler)
        {
            m_handler = handler;

            m_pool = new StreamPool();

            
        }

        


        async Task<T> Internal_SendAsync<T>(Uri uri, MHttpRequest request, CancellationToken token, Func<MHttpResponse, T> translateFunc)
        {
            
            try
            {
                var writer = m_pool.Find(m_handler, uri);

                var requestFunc = request.CreateSendAsync();

                while (true)
                {
                    var pack = new RequestPack(token, requestFunc);

                    await writer.WriteAsync(pack).ConfigureAwait(false);

                    MHttpResponse response;

                    try
                    {
                        response = await pack.Task.ConfigureAwait(false);
                    }
                    catch (IOException)
                    {
                        continue;
                    }
                    catch (ObjectDisposedException)
                    {
                        continue;
                    }

                    return translateFunc(response);
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

        Task<T> GetRefererAsync<T>(Uri uri, Uri referer, CancellationToken token, Func<MHttpResponse, T> func)
        {
            MHttpRequest request = MHttpRequest.CreateGet(uri);

            request.Headers.Set("Referer", referer.AbsoluteUri);

            return Internal_SendAsync(uri, request, token, (response) =>
            {



                ChuckResponseStatus(response);

                return func(response);
            });
        }

        public Task<Stream> GetStreamAsync(Uri uri, Uri referer, CancellationToken token)
        {
            return GetRefererAsync(uri, referer, token, (res) => res.Content.GetStream());
        }

        public Task<byte[]> GetByteArrayAsync(Uri uri, Uri referer, CancellationToken token)
        {
            return GetRefererAsync(uri, referer, token, (res) => res.Content.GetByteArray());
            

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