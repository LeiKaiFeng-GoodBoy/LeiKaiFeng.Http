using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LeiKaiFeng.Http
{

    public sealed class MHttpResponse : MHttpMessage
    {


        internal MHttpResponse(int status, MHttpHeaders headers) : base(headers, new MHttpContent())
        {
            Status = status;

        }


        public int Status { get; private set; }




        public static MHttpResponse Create(int status)
        {
            return new MHttpResponse(status, new MHttpHeaders());
        }

        Func<ReadParameter, ReadResult<MHttpResponse>> ReadContentAsync(MHttpStream stream, int maxContentSize)
        {
            return Content.ReadAsync(stream, Headers, this, maxContentSize);
        }

        public static ValueTask<MHttpResponse> ReadAsync(MHttpStream stream, int maxContentSize)
        {

            Func<ReadParameter, ReadResult<MHttpResponse>> func = MHttpStream.ReadHttpHeaders(ReadResultStatus.Used_NotResult, (buffer) =>
            {
                var response = MHttpResponse.CreateMHttpResponse(buffer);

                func = response.ReadContentAsync(stream, maxContentSize);

                return response;
            });

            return stream.ReadAsync((parameter) => func(parameter));
        }

        static int ParseResponseStatus(string firstLine)
        {
            string[] ss = firstLine.Split(new char[] { ' ' }, 3, StringSplitOptions.RemoveEmptyEntries);

            return int.Parse(ss[1]);
        }

        internal static MHttpResponse CreateMHttpResponse(ArraySegment<byte> buffer)
        {

            var firstDic = MHttpStream.ParseLine(buffer);

            int status = ParseResponseStatus(firstDic.Key);

            return new MHttpResponse(status, firstDic.Value);

        }

        internal Task SendHeadersAsync(MHttpStream stream)
        {
            string firstLine = $"HTTP/1.1 {Status} OK";

            byte[] buffer = MHttpStream.EncodeHeaders(firstLine, Headers.Headers);

            return stream.WriteAsync(buffer);
        }


        public async Task SendAsync(MHttpStream stream)
        {
            await SendHeadersAsync(stream).ConfigureAwait(false);

            await Content.SendAsync(stream).ConfigureAwait(false);
        }
    }
}