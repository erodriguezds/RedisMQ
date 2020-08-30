@echo off 
docker run -d --rm -v %CD%:/usr/src/mymodule -w /usr/src/mymodule --name=redis-sdk redis-sdk
pause
