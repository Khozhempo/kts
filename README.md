# KTS
Program part of LTO tape library.

KTS work with LTO without ltfs and provide virtual file system (with long time cache) for user. Also has function to save over copies (more than one) of data.

LTO-1(2..4) do not have file system and needs special knowledgs and software to write/read data from tape. This program create virtual file system, where shows the files on tapes. When you wants to download a file, just copy and wait while it will be get from tape. While not ready jet the hardware component (library), it works with one tape in drive or in test mode (files store on HDD).


INSTALLATION
------------

1. Download
2. Compile
3. Create mySQL base
4. Change mysql connection in config.json
5. Create folder for virtual FS (I use /mnt/tapes)
6. Run kts MOUNTPOINT (-t optional), example kts /mnt/tapes
7. Browse /mnt/tapes...


REQUIREMENTS
------------

Ubuntu. LTO tape drive optional. :) In test mode it works as emulator.


FEATURES
--------

In real (not emulate) mode when you request the file, system return you the error that file cannot be opened. This is done so as not to keep you waiting the finish of downloading (which can go on more then 5 minutes). System return you the error, but immideatly start to copy file from tape to HDD. When it will be ready, the file(s) status will be changed to executable (+x). You see it highlighted in MC (Midnight Commander or ls).