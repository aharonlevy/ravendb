$TARGET_SPECS = (
    @{
        "Name"      = "windows-x64";
        "Runtime"   = "win-x64";
        "PkgType"   = "zip";
        "IsUnix"    = $False;
        "TargetId" = "win-x64";
    },
    @{
        "Name"      = "windows-x86";
        "Runtime"   = "win-x86";
        "Arch"      = "x86";
        "PkgType"   = "zip";
        "IsUnix"    = $False;
        "TargetId" = "win-x86";
    },
    @{
        "Name"      = "linux-x64";
        "Runtime"   = "linux-x64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "linux-x64";
    },
    @{
        "Name"      = "macos-x64";
        "Runtime"   = "osx-x64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "macos";
    },
    @{
        "Name"      = "raspberry-pi";
        "Runtime"   = "linux-arm";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "rpi"
    },
     @{
        "Name"      = "arm-64";
        "Runtime"   = "linux-arm64";
        "PkgType"   = "tar.bz2";
        "IsUnix"    = $True;
        "TargetId" = "arm64"
    }
);

function GetBuildTargets( $targets ) {

    if (($targets -eq $null) -or ($targets.Count -eq 0)) {
        return $TARGET_SPECS;
    }

    $result = @( );

    foreach ($spec in $TARGET_SPECS) {
        foreach ($target in $targets) {
            if ($spec.TargetId -eq $target.ToLowerInvariant()) {
                $result += $spec
            }
        }
    }

    return $result;
} 
