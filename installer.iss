[Setup]
AppName=FB私聊助手
AppVersion=1.0.0
DefaultDirName={autopf}\FB私聊助手
DefaultGroupName=FB私聊助手
OutputDir=Output
OutputBaseFilename=FB私聊助手安装包
Compression=lzma
SolidCompression=yes
PrivilegesRequired=admin
WizardStyle=modern
SetupIconFile=assets\fb_chat_helper.ico
UninstallDisplayIcon={app}\FB_RPA_Client.exe

[Tasks]
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加任务:"; Flags: unchecked

[Files]
Source: "dist\FB_RPA_Client\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\FB私聊助手"; Filename: "{app}\FB_RPA_Client.exe"
Name: "{autodesktop}\FB私聊助手"; Filename: "{app}\FB_RPA_Client.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\FB_RPA_Client.exe"; Description: "安装完成后立即启动"; Flags: nowait postinstall skipifsilent
