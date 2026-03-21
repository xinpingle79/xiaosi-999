param(
  [string]$PackageRoot = "dist/FB_RPA_Client",
  [string]$OutputDir = "dist/windows-visual-audit"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type @"
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

public static class WindowAudit {
    public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);

    [DllImport("user32.dll")] public static extern bool EnumWindows(EnumWindowsProc lpEnumFunc, IntPtr lParam);
    [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int count);
    [DllImport("user32.dll")] public static extern int GetClassName(IntPtr hWnd, StringBuilder text, int count);
    [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);

    public static List<Dictionary<string, string>> GetVisibleWindows(uint processId) {
        var results = new List<Dictionary<string, string>>();
        EnumWindows(delegate (IntPtr hWnd, IntPtr lParam) {
            uint pid;
            GetWindowThreadProcessId(hWnd, out pid);
            if (pid != processId) return true;
            if (!IsWindowVisible(hWnd)) return true;

            var title = new StringBuilder(512);
            var klass = new StringBuilder(256);
            GetWindowText(hWnd, title, title.Capacity);
            GetClassName(hWnd, klass, klass.Capacity);

            results.Add(new Dictionary<string, string> {
                { "title", title.ToString() },
                { "class_name", klass.ToString() }
            });
            return true;
        }, IntPtr.Zero);
        return results;
    }
}
"@

function Save-DesktopScreenshot {
  param([string]$Path)
  try {
    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing
    $bounds = [System.Windows.Forms.Screen]::PrimaryScreen.Bounds
    $bmp = New-Object System.Drawing.Bitmap $bounds.Width, $bounds.Height
    $graphics = [System.Drawing.Graphics]::FromImage($bmp)
    $graphics.CopyFromScreen($bounds.Location, [System.Drawing.Point]::Empty, $bounds.Size)
    $bmp.Save($Path, [System.Drawing.Imaging.ImageFormat]::Png)
    $graphics.Dispose()
    $bmp.Dispose()
    return $true
  } catch {
    return $false
  }
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$targets = @(
  @{ name = "client"; path = (Join-Path $PackageRoot "FB_RPA_Client.exe") },
  @{ name = "worker"; path = (Join-Path $PackageRoot "FB_RPA_Worker.exe") },
  @{ name = "main"; path = (Join-Path $PackageRoot "FB_RPA_Main.exe") }
)

$results = @()
foreach ($target in $targets) {
  if (-not (Test-Path $target.path)) {
    $results += @{
      name = $target.name
      path = $target.path
      exists = $false
    }
    continue
  }

  $proc = $null
  try {
    $proc = Start-Process -FilePath $target.path -PassThru
    Start-Sleep -Seconds 3
    $windows = [WindowAudit]::GetVisibleWindows([uint32]$proc.Id)
    $screenshotPath = Join-Path $OutputDir "$($target.name).png"
    $shotSaved = Save-DesktopScreenshot -Path $screenshotPath
    $results += @{
      name = $target.name
      path = $target.path
      exists = $true
      pid = $proc.Id
      has_visible_windows = ($windows.Count -gt 0)
      windows = $windows
      screenshot_saved = $shotSaved
      screenshot = (if ($shotSaved) { $screenshotPath } else { "" })
    }
  } finally {
    if ($proc -and -not $proc.HasExited) {
      Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
  }
}

$jsonPath = Join-Path $OutputDir "windows_visual_audit.json"
$results | ConvertTo-Json -Depth 5 | Set-Content -Path $jsonPath -Encoding UTF8
Write-Host "windows_visual_audit: wrote $jsonPath"
