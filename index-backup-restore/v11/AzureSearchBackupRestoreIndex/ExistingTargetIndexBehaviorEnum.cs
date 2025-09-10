using System.Runtime.Serialization;

namespace AzureSearchBackupRestoreIndex;

public enum ExistingTargetIndexBehaviorEnum
{

    [EnumMember(Value = "Merge")]
    Merge = 0,
    [EnumMember(Value = "Delete")]
    Delete
}
