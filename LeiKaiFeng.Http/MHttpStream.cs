using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Buffers.Text;
using System.Runtime.InteropServices;

namespace LeiKaiFeng.Http
{

    [Serializable]
    public sealed class MHttpNotImplementedException : Exception
    {
        public MHttpNotImplementedException(string message) : base(message)
        {

        }
    }

    [Serializable]
    public sealed class MHttpStreamException : Exception
    {

        public MHttpStreamException(string message) : base(message)
        {

        }
    }


    internal enum ReadResultStatus
    {
        NotUsed_NotResult = 0,

        Used_NotResult,

        Used_Result
    }

    internal readonly struct ReadResult<T>
    {
       

        public ReadResult(ReadResultStatus status, int usedCount, T result)
        {
            Status = status;
          
            UsedCount = usedCount;
            
            Result = result;
        }

        public static ReadResult<T> Create(int usedCount, T result)
        {
            return new ReadResult<T>(ReadResultStatus.Used_Result, usedCount, result);
        }


        public static ReadResult<T> Create(int usedCount)
        {
            return new ReadResult<T>(ReadResultStatus.Used_NotResult, usedCount, default);
        }


        public static ReadResult<T> Create()
        {
            return new ReadResult<T>(ReadResultStatus.NotUsed_NotResult, 0, default);
        }


        public ReadResultStatus Status { get; }

        public int UsedCount { get; }

        public T Result { get; }
    }

    internal readonly struct ReadParameter
    {
        public ReadParameter(byte[] buffer, int offset, int count)
        {
            Buffer = buffer;
            Offset = offset;
            Count = count;
        }

        public byte[] Buffer { get; }

        public int Offset { get; }


        public int Count { get; }

    }



    //从索引0-UsedOffset是没有使用的字节数
    //从索引UsedOffset-ReadOffset是已经读取的字节数
    //从索引ReadOffset-MaxOffset是可以读取的字节数
    //没有实现为一个环形缓冲区，但会将已经读取的字节左移


    public sealed partial class MHttpStream
    {
        sealed class StreamWarp
        {
            public StreamWarp()
            {
                Read = CreateDelete(nameof(Stream.ReadAsync), Memory<byte>.Empty, CancellationToken.None, new ValueTask<int>());

                if (Read is null)
                {
                   
                    Read = (stream, memory, token) =>
                    {

                        if (MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> array))
                        {
                            var task = stream.ReadAsync(array.Array, array.Offset, array.Count, token);

                            return new ValueTask<int>(task);
                        }
                        else
                        {
                            throw new NotImplementedException();
                        }

                            
                    };
                }
                

                Write = CreateDelete(nameof(Stream.WriteAsync), ReadOnlyMemory<byte>.Empty, CancellationToken.None, new ValueTask());

                if (Write is null)
                {
                    
                    Write = (stream, memory, token) =>
                    {
                        if (MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> array))
                        {
                            var task = stream.WriteAsync(array.Array, array.Offset, array.Count, token);

                            return new ValueTask(task);
                        }
                        else
                        {
                            throw new NotImplementedException();
                        }

                            
                    };
                }
                
            }

            public Func<Stream, Memory<byte>, CancellationToken, ValueTask<int>> Read { get; }

            public Func<Stream, ReadOnlyMemory<byte>, CancellationToken, ValueTask> Write { get; }



            static Func<Stream, T1, T2, T3> CreateDelete<T1, T2, T3>(string name, T1 t1, T2 t2, T3 t3)
            {
                var method = typeof(Stream).GetMethod(name, new Type[] { typeof(T1), typeof(T2) });

                TN F<TN>()
                {
                    return (TN)(object)Delegate.CreateDelegate(typeof(TN), method, false);
                }

                if (method is null)
                {
                    return null;
                }
                else
                {
                    return F<Func<Stream, T1, T2, T3>>();
                }
                
            }

        }

        static readonly StreamWarp s_streamWarp = new StreamWarp();

       
       
        const int MAX_SIZE_65536 = 65536;

        readonly Socket m_socket;

        readonly Stream m_stream;
       
        readonly byte[] m_buffer;

        int m_read_offset;

        int m_used_offset;

        int m_max_chunked_content_length = 1024;

        int CanReadSize => m_buffer.Length - m_read_offset;

        int CanUsedSize => m_read_offset - m_used_offset;

        public MHttpStream(Socket socket, Stream stream) : this(socket, stream, MAX_SIZE_65536)
        {

        }

        public MHttpStream(Socket socket, Stream stream, int maxSize)
        {
            m_socket = socket;

            m_stream = stream;

            m_buffer = new byte[maxSize];

            m_read_offset = 0;

            m_used_offset = 0;
        }



        void Move()
        {
            int used_size = CanUsedSize;

            if (used_size == m_buffer.Length)
            {
                throw new MHttpStreamException("无法在有限的缓冲区中找到协议的边界，请增大缓冲区重试");
            }
            else
            {
                Buffer.BlockCopy(m_buffer, m_used_offset, m_buffer, 0, used_size);



                m_read_offset = used_size;

                m_used_offset = 0;
            }
        }


        internal async ValueTask<T> ReadAsync<T>(Func<ReadParameter, ReadResult<T>> func)
        {
            bool isHaveResult(out T result)
            {
                while (CanUsedSize > 0)
                {
                    var readResult = func(new ReadParameter(m_buffer, m_used_offset, CanUsedSize));

                    if (readResult.Status == ReadResultStatus.Used_NotResult)
                    {
                        //没有产生结果可能不需要从流中读取,继续while检测CanUsedSize
                        //如果为零则结束循环Move移动缓冲区且调整索引

                        m_used_offset += readResult.UsedCount;
                    }
                    else if (readResult.Status == ReadResultStatus.Used_Result)
                    {
                        m_used_offset += readResult.UsedCount;

                        result = readResult.Result;

                        return true;
                    }
                    else
                    {
                        result = default;

                        return false;
                    }
                }

                result = default;

                return false;
            }

            T out_result;
            while (isHaveResult(out out_result) == false) 
            {
                Move();

                int n = await s_streamWarp.Read(m_stream, m_buffer.AsMemory(m_read_offset, CanReadSize), default).ConfigureAwait(false);

                if (n <= 0)
                {
                    throw new IOException("协议未完整读取");
                }
                else
                {
                    m_read_offset += n;
                }  
            }

            return out_result;
        }

        static int GetChunkedEndMarkLength()
        {
            return 2;
        }

        static bool IsFindOneMark(ReadParameter parameter, out int index, out int markLength)
        {
            ReadOnlySpan<byte> mark = stackalloc byte[] { (byte)'\r', (byte)'\n' };

            return IsFindMark(parameter, mark, out index, out markLength);
        }

        static bool IsFindTowMark(ReadParameter parameter, out int index, out int markLength)
        {
            ReadOnlySpan<byte> mark = stackalloc byte[] { (byte)'\r', (byte)'\n', (byte)'\r', (byte)'\n' };

            return IsFindMark(parameter, mark, out index, out markLength);
        }

        static bool IsFindMark(ReadParameter parameter, ReadOnlySpan<byte> mark, out int index, out int markLength)
        {
            
            markLength = mark.Length;

            index = parameter.Buffer.AsSpan(parameter.Offset, parameter.Count).IndexOf(mark);

            if (index == -1)
            {
               
                return false;
            }
            else
            {
                return true;
            }




        }

        internal static Func<ReadParameter, ReadResult<T>> ReadHttpHeaders<T>(ReadResultStatus status, Func<ArraySegment<byte>, T> func)
        {
            return (parameter) =>
            {

                if (IsFindTowMark(parameter, out int index, out int markLength))
                {
                    index += markLength;

                    return new ReadResult<T>(
                        status,
                        index,
                        func(new ArraySegment<byte>(parameter.Buffer, parameter.Offset, index)));
                    
                }
                else
                {
                    return ReadResult<T>.Create();
                }
            };        
        }



        internal Func<ReadParameter, ReadResult<T>> ReadByteArrayAsync<T>(T value, Action<byte[]> result, int size, int maxContentSize)
        {
            if ((uint)size > maxContentSize)
            {
                throw new MHttpStreamException("内容长度大于限制长度");
            }

            byte[] buffer = new byte[size];

            int offset = 0;

            return (parameter) =>
            {
                int n;

                if(parameter.Count > size)
                {
                    n = size;
                }
                else
                {
                    n = parameter.Count;
                }

                parameter.Buffer.AsSpan(parameter.Offset, n).CopyTo(buffer.AsSpan(offset));

                offset += n;

                size -= n;

                if (size <= 0)
                {
                    result(buffer);

                    return ReadResult<T>.Create(n, value);
                }
                else
                {
                    return ReadResult<T>.Create(n);
                }
            };
        }

        MemoryStream CreateChunkedContentStream()
        {
            return new MemoryStream(m_max_chunked_content_length);
        }

        void SetMaxChunkedContentLength(MemoryStream stream)
        {
            m_max_chunked_content_length = Math.Max(m_max_chunked_content_length, checked((int)stream.Length));
        }

        internal Func<ReadParameter, ReadResult<T>> ReadChunkedContentAsync<T>(T value, Action<MemoryStream> result, int maxContentSize)
        {
            MemoryStream stream = CreateChunkedContentStream();

            Func<ReadParameter, ReadResult<T>> func = readChunkedLength;

            ReadResult<T> readChunkedLength(ReadParameter parameter)
            {

                if (IsFindOneMark(parameter, out int index, out int markLength) == false)
                {
                    return ReadResult<T>.Create();
                }
                else
                {
                    if (Utf8Parser.TryParse(parameter.Buffer.AsSpan(parameter.Offset, index), out int length, out int used_s_size, 'X') &&
                        length >= 0 &&
                        used_s_size == index)
                    {
                        int sumLength = checked((int)stream.Position + length);

                        if (sumLength > maxContentSize)
                        {
                            throw new MHttpStreamException("内容长度大于限制长度");
                        }
                        else
                        {
                            func = createReadChunkedFunc(length);

                            return ReadResult<T>.Create(index + markLength);
                        }                   
                    }
                    else
                    {
                        throw new MHttpStreamException("Chunked Length Error");
                    }
                }
            }

            Func<ReadParameter, ReadResult<T>> createReadChunkedFunc(int size)
            {
                int count = checked(size + GetChunkedEndMarkLength());

                return (parameter) =>
                {

                    int n;

                    if (parameter.Count > count)
                    {
                        n = count;
                    }
                    else
                    {
                        n = parameter.Count;

                    }

                    stream.Write(parameter.Buffer, parameter.Offset, n);

                    count -= n;

                    if (count > 0)
                    {
                        return ReadResult<T>.Create(n);
                    }
                    else
                    {
                        stream.Position -= GetChunkedEndMarkLength();

                        if (size == 0)
                        {
                            stream.Position = 0;

                            SetMaxChunkedContentLength(stream);

                            result(stream);

                            return ReadResult<T>.Create(n, value);
                        }
                        else
                        {
                            func = readChunkedLength;

                            return ReadResult<T>.Create(n);
                        }
                    }

                };

            }

            return (parameter) => func(parameter);
        }

        internal Task WriteAsync(byte[] buffer)
        {
            return this.WriteAsync(buffer, 0, buffer.Length);
        }

        internal Task WriteAsync(byte[] buffer, int offset, int count)
        {
            return s_streamWarp.Write(m_stream, buffer.AsMemory(offset, count), default).AsTask();
        }

        public void Close()
        {
            
            m_stream.Close();

            m_socket.Close();
        }

        public void Cencel()
        {
            m_socket.Close();
        }
    }

    public sealed partial class MHttpStream
    {
        static string ParseOneLine(string headers, ref int offset)
        {
            const string c_mark = "\r\n";

            int index = headers.IndexOf(c_mark, offset, StringComparison.OrdinalIgnoreCase);

            if (index == -1)
            {
                throw new MHttpStreamException("解析HTTP头出错");
            }
            else
            {
                string s = headers.Substring(offset, index - offset);

                offset = index + c_mark.Length;

                return s;
            }

        }

        static KeyValuePair<string, string> ParseKeyValue(string s)
        {
            const string c_mark = ":";

            int index = s.IndexOf(c_mark, StringComparison.OrdinalIgnoreCase);

            if (index == -1)
            {
                throw new MHttpStreamException("解析HTTP头出错");
            }
            else
            {
                string key = s.Substring(0, index).Trim();

                string value = s.Substring(index + c_mark.Length).Trim();

                return new KeyValuePair<string, string>(key, value);
            }
        }

        internal static KeyValuePair<string, MHttpHeaders> ParseLine(ArraySegment<byte> buffer)
        {
            string headers = Encoding.UTF8.GetString(buffer.Array, buffer.Offset, buffer.Count);

            int offset = 0;

            string first_line = ParseOneLine(headers, ref offset);


            var dic = new MHttpHeaders();

            while (true)
            {
                string s = ParseOneLine(headers, ref offset);

                if (s.Length == 0)
                {
                    return new KeyValuePair<string, MHttpHeaders>(first_line, dic);
                }
                else
                {
                    var keyValue = ParseKeyValue(s);

                    dic.Set(keyValue.Key, keyValue.Value);
                }

            }


        }


        internal static byte[] EncodeHeaders(string firstLine, Dictionary<string, string> headers)
        {
            const string c_mark = "\r\n";

            StringBuilder sb = new StringBuilder(2048);

            sb.Append(firstLine).Append(c_mark);

            foreach (var item in headers)
            {
                sb.Append(item.Key).Append(": ").Append(item.Value).Append(c_mark);
            }

            sb.Append(c_mark);

            string s = sb.ToString();

            return Encoding.UTF8.GetBytes(s);
        }

        

    }
}