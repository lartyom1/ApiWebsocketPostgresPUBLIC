using System.Drawing.Imaging;
using System.Drawing;

public static class BitmapExtensions
{
    public static void SaveJPG(this Bitmap bmp, string filename)
    {
        EncoderParameters encoderParameters = new EncoderParameters(1);
        encoderParameters.Param[0] = new EncoderParameter(System.Drawing.Imaging.Encoder.Quality, 90L);
        bmp.Save(filename, GetEncoder(ImageFormat.Jpeg), encoderParameters);
    }

    public static ImageCodecInfo GetEncoder(ImageFormat format)
    {
        ImageCodecInfo[] codecs = ImageCodecInfo.GetImageDecoders();

        foreach (ImageCodecInfo codec in codecs)
        {
            if (codec.FormatID == format.Guid)
            {
                return codec;
            }
        }

        return null;
    }
}