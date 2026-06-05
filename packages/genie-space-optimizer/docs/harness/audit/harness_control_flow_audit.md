# Harness Control-Flow Audit

Function: `_run_lever_loop`  
Line range: 17986–18065  
Total branch points: 3  

## Reachability summary

* `airline_run_59a173d3`: 4636 lines executed
* `seven_now_run_ab65fefe`: 4689 lines executed

## Branch points

| lineno | type | depth | parent | detail | reached:airline_run_59a173d3 | reached:seven_now_run_ab65fefe | snippet |
|---|---|---|---|---|---|---|---|
| 18035 | if | 0 | module |  | YES | YES | `if _legacy:` |
| 18036 | return | 1 | if |  | YES | YES | `return _run_lever_loop_legacy(` |
| 18051 | return | 0 | module |  | no | no | `return _run_lever_loop_sm_first(` |
