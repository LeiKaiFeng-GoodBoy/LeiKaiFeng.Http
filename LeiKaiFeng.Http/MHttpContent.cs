using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;

namespace LeiKaiFeng.Http
{
    public sealed class MHttpContent
    {
        object m_content;


        public MHttpContent()
        {
            m_content = Array.Empty<byte>();
        }

        public bool IsEmptyContent()
        {
            if(m_content is MemoryStream stream)
            {
                if (stream.Length == 0)
                {
                   
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                var buffer = (byte[])m_content;


                if (buffer.Length == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }


        public Stream GetStream()
        {
            byte[] buffer = m_content as byte[];

            if (buffer is null)
            {
                return (Stream)m_content;
            }
            else
            {
                return new MemoryStream(buffer);
            }
        }




        public byte[] GetByteArray()
        {
            byte[] buffer = m_content as byte[];

            if (!(buffer is null))
            {
                return buffer;
            }
            else
            {
                MemoryStream memoryStream = ((MemoryStream)m_content);

                buffer = memoryStream.GetBuffer();

                if (buffer.Length == memoryStream.Length)
                {
                    return buffer;
                }
                else
                {
                    Array.Resize(ref buffer, checked((int)memoryStream.Length));

                    return buffer;
                }
            }


        }

        public string GetString()
        {
            return Encoding.UTF8.GetString(GetByteArray());
        }

        internal void SetByteArray(byte[] array)
        {
            m_content = array;
        }

        static MemoryStream DecompressStream(Stream stream)
        {
            MemoryStream memoryStream = new MemoryStream();

            stream.CopyTo(memoryStream);

            //memoryStream.SetLength(memoryStream.Position);

            memoryStream.Position = 0;

            return memoryStream;
        }

        static MemoryStream CreateStream(object obj)
        {
            MemoryStream memoryStream = obj as MemoryStream;

            if(memoryStream is null)
            {
                return new MemoryStream((byte[])obj);
            }
            else
            {
                return memoryStream;
            }
        }

        void SetIdentity(object obj)
        {
            m_content = obj;

        }

        void SetDeflate(object obj)
        {
            Stream stream = CreateStream(obj);

           
            m_content = DecompressStream(new DeflateStream(stream, CompressionMode.Decompress));

        }


        void SetGZip(object obj)
        {
            Stream stream = CreateStream(obj);


            m_content = DecompressStream(new GZipStream(stream, CompressionMode.Decompress));

        }

        Action<object> CreateSetFunc(MHttpHeaders headers)
        {


            if (headers.Headers.TryGetValue(MHttpHeadersKey.ContentEncoding, out string s))
            {
                if (-1 != s.IndexOf("gzip", StringComparison.OrdinalIgnoreCase))
                {
                    return SetGZip;
                }
                else if (-1 != s.IndexOf("deflate", StringComparison.OrdinalIgnoreCase))
                {
                    return SetDeflate;
                }
                else if (-1 != s.IndexOf("identity", StringComparison.OrdinalIgnoreCase))
                {
                    return SetIdentity;
                }
                else
                {
                    throw new MHttpStreamException("内容编码不支持");
                }
            }
            else
            {
                return SetIdentity;
            }
        }


        internal Func<ReadParameter, ReadResult<T>> ReadAsync<T>(MHttpStream stream, MHttpHeaders headers, T value, int maxContentLength)
        {

            if (headers.IsChunked())
            {
                return stream.ReadChunkedContentAsync(value, CreateSetFunc(headers), maxContentLength);
            }
            else if (headers.TryGetContentLength(out int length))
            {
                return stream.ReadByteArrayAsync(value, CreateSetFunc(headers), length, maxContentLength);
            }
            else
            {
                throw new MHttpNotImplementedException("没有明确长度的分支未实现");
            }
        }

        internal Task SendAsync(MHttpStream stream)
        {
            byte[] buffer = m_content as byte[];

            if(buffer is null)
            {
                MemoryStream memoryStream = (MemoryStream)m_content;

                int length = checked((int)memoryStream.Length);

                return stream.WriteAsync(memoryStream.GetBuffer(), 0, length);
            }
            else
            {
                return stream.WriteAsync(buffer, 0, buffer.Length);
            }
        }
    }
}