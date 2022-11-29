from cx_Freeze import setup, Executable

base = None    

executables = [Executable("sender.py", base=base)]

packages = ["idna"]
options = {
    'build_exe': {    
        'packages':packages,
    },    
}

setup(
    name = "API WhatsTalk",
    options = options,
    version = "1.00.001",
    description = 'First Version for API Integration',
    executables = executables
)