[Setup]
AppName=小四客户端
AppVersion=1.0.0
DefaultDirName={autopf}\小四客户端
DefaultGroupName=小四客户端
OutputDir=Output
OutputBaseFilename=小四客户端安装包
Compression=lzma
SolidCompression=yes
PrivilegesRequired=admin
WizardStyle=modern

[Tasks]
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加任务:"; Flags: unchecked

[Files]
Source: "dist\FB_RPA_Client\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\小四客户端"; Filename: "{app}\FB_RPA_Client.exe"
Name: "{autodesktop}\小四客户端"; Filename: "{app}\FB_RPA_Client.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\FB_RPA_Client.exe"; Description: "安装完成后立即启动"; Flags: nowait postinstall skipifsilent
