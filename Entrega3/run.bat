@echo off
echo Subindo servidor e clientes...
 
start "Servidor" cmd /k "cd /d %~dp0servidor & python3 servidor.py"
timeout /t 1 >nul
start "Cliente1" cmd /k "cd /d %~dp0cliente & python3 cliente.py"
start "Cliente2" cmd /k "cd /d %~dp0cliente & python3 cliente.py"
start "Cliente3" cmd /k "cd /d %~dp0cliente & python3 cliente.py"