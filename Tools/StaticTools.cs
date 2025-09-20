using System.Text;

namespace Tools;

public static class StaticTools
{
    /// <summary>
    /// create a random string of the specified length
    /// </summary>
    /// <param name="length"></param>
    /// <param name="invalidChars">character not to be included</param>
    /// <returns></returns>
    public static string RandomString(int length, char[]? invalidChars = null)
    {
        // use unicode characters
        var sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            do
            {
                var c = Random.Shared.Next(0x20, 0xD7FF);
                if (invalidChars != null && Array.IndexOf(invalidChars, (char)c) >= 0)
                {
                    continue;
                }

                sb.Append(c);
                break;

            } while (true);
        }
        return sb.ToString();
    }

}
