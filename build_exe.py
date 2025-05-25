import sys
from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {
    "packages": ["os", "sys", "configparser", "pandas", "pyodbc", "datetime", "getpass", "odf"],
    "excludes": ["tkinter"],
    "include_files": ["config.ini"]
}

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
if sys.platform == "win32":
    base = "Win32GUI"

setup(
    name="ods_to_mssql",
    version="1.0",
    description="Загрузчик данных из ODS в MSSQL",
    options={"build_exe": build_exe_options},
    executables=[Executable("ods_to_mssql.py", base=base)]
)