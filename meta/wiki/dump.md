
## Metrics container drop matters?

Drop container null:
* springstarter 1.2MB
* ts auth mongo 4.4.15 86.2 kB

not Drop container null:
* springstarter 1.2 MB
* ts auth mongo 4.4.15 86.2 kB

no difference --> solution, drop container at end: data preserved and non matching values get dropped

drop at end:
* springstarter 805kB
* ts-auth-mongo 4.4.15 86.2 kB