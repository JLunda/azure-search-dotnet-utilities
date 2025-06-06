using System.Runtime.Serialization;

namespace AzureSearchBackupRestoreIndex;

public enum AzureCloudEnum
{

    [EnumMember(Value = "Public")]
    AzurePublicCloud = 0,
    [EnumMember(Value = "U.S. Government")]
    AzureUSGovernment
}
