Set objShell = WScript.CreateObject("WScript.Shell")

' Ativar o ambiente virtual e executar o script Python
objShell.Run "cmd.exe /K venv\Scripts\activate && python app.py", 1, False
