import sys
from cx_Freeze import setup, Executable


build_exe_options = {
    "packages": ["os", "sys", "configparser", "pandas", "pyodbc", "datetime", "getpass", "odf"],
    "excludes": ["tkinter"],
    "include_files": ["config.ini"]
}
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