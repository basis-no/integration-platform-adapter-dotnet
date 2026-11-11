using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Basis.IntegrationPlatform.MessageStore;

/// <summary>
/// The message id derived from a Kafka record's position, the same in both
/// adapters: the UUID made from the first 16 bytes of SHA-256 over
/// <c>kafka-record LF integration LF topic LF partition LF offset</c>, with the
/// version and variant bits of a name-based UUID. A record whose own
/// <c>x-bip-message-id</c> the Message Store cannot hold is parked and recorded
/// under it.
/// </summary>
internal static class PlatformRecordIdentity
{
    internal static string Derive(string integration, string topic, int partition, long offset)
    {
        var text = string.Create(CultureInfo.InvariantCulture, $"kafka-record\n{integration}\n{topic}\n{partition}\n{offset}");
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        digest[6] = (byte)((digest[6] & 0x0f) | 0x50);
        digest[8] = (byte)((digest[8] & 0x3f) | 0x80);
        // From the bytes in order: new Guid(byte[]) would reorder the first
        // three groups.
        var hex = Convert.ToHexString(digest, 0, 16).ToLowerInvariant();
        return $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
    }
}
