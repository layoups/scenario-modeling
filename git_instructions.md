## Git Best Practices

local --> github
local --> CSR --> VM

VM --> CSR --> Local --> Github

## Change in VM
VM: git push to CSR
local: git pull google <branch>
local: git push to Github


## Change in Local
local: git push to Github
local: git push --all google to CSR
VM: git pull

## Suppress Warnings
- python -W ignore <file.py>

